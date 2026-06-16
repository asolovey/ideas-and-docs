# Apache Iceberg Table Replica — Requirements

## 1. Problem Statement

Apache Iceberg table files store **absolute** S3 URLs internally. Files copied to a different S3
bucket via replication are therefore non-functional without path correction: the metadata files,
manifest lists, and manifests all reference the original bucket's absolute paths, and position
delete files additionally embed absolute data file paths in their `_path` column.

This project implements an incremental replication service that:

- Relies on pre-existing AWS S3 bucket replication for file transfer between sites
- Detects when replicated files have arrived at the destination
- Rewrites Iceberg metadata, manifest, and position delete files with corrected paths
- Maintains a replica JDBC catalog pointing to the rewritten, fully-functional files
- Bridges the structural lag between JDBC catalog replication (seconds) and S3 file
  replication (up to 15 minutes per AWS SLA)

### 1.1 Out of Scope

- Apache Iceberg specification V4 (unreleased): introduces relative links and would eliminate
  this problem. This project addresses tables written using older specifications only.
- AWS S3 Multi-Region Access Points (MRAP): explicitly excluded. No AWS-specific technology
  beyond basic S3 operations is used.
- Non-Parquet data file formats: Parquet is assumed for all data files.
- Primary S3 location access from the replica service: the replica service cannot read from
  the primary S3 bucket.


## 2. Assumptions and Prerequisites

The following are operator responsibilities and preconditions for this system:

- AWS S3 replication is configured and operational between primary and replica S3 locations
  before this service is deployed.
- The S3 replication scope (bucket/prefix filters) is configured to cover **only** the primary's
  original data location, explicitly **excluding** the replica's write locations
  (`write.metadata.path`, `write.data.path`). This prevents replication loops.
- On fail-over and fail-back events, the operator adjusts S3 replication scope accordingly
  (see Section 8). The replication service does not validate or enforce this.
- The same `S3FileIO` instance and AWS credentials can access both the replicated S3 location
  and the replica write locations.
- PostgreSQL 13+ is used for all JDBC catalog instances.


## 3. Architecture Overview

### 3.1 Sites

The system operates across two sites (extendable to more replicas per primary):

**Primary Site:**
- Primary PostgreSQL (writable): Iceberg JDBC catalog + metadata history tracking
- Primary S3: original Iceberg table files

**Replica Site(s):**
- Read-only PostgreSQL: mirrors primary catalog including metadata history; polled by
  the replication service
- Writable PostgreSQL: replica's own Iceberg catalog, job queue, and operational state;
  written exclusively by the replication service
- Replicated S3 location: S3-replicated copy of primary files; read-only to this service
- Write location (`write.metadata.path`, `write.data.path`): destination for rewritten
  files; separate S3 bucket or prefix; **never** in S3 replication scope

### 3.2 Multiple Replicas

Multiple replicas per primary are supported. Each replica:

- Has a unique UUID (replica ID) stored in its own writable PostgreSQL instance
- Uses the same Iceberg catalog name as the primary
- Runs its own independent replication service instance
- Reads independently from the shared read-only replica of the primary PostgreSQL catalog
- Maintains its own job queue and tracking state in its writable PostgreSQL

### 3.3 S3 Location Invariant

The separation between replicated location and write locations is architecturally critical.
Rewritten files written to `write.metadata.path` or `write.data.path` must never enter the
S3 replication scope, as they contain site-specific absolute paths and would be meaningless
(or actively harmful) if replicated to another site.


## 4. PostgreSQL Architecture

Three distinct PostgreSQL roles exist in the system.

### 4.1 Primary PostgreSQL (Writable, at Primary Site)

Standard Iceberg JDBC catalog tables (pre-existing, unmodified):
- `iceberg_tables`
- `iceberg_namespace_properties`

New objects added by the SQL initialization script (Section 5):
- `iceberg_metadata_history` table: one row per `metadata_location` change per table,
  with a monotonic sequence number and timestamp
- Trigger on `iceberg_tables`: `AFTER INSERT OR UPDATE OF metadata_location OR DELETE` fires and
  inserts a row into `iceberg_metadata_history`

All tables above are replicated to the read-only replica via PostgreSQL replication.

### 4.2 Read-Only PostgreSQL (at Replica Site)

Mirrors all primary tables via PostgreSQL replication:
- `iceberg_tables`
- `iceberg_namespace_properties`
- `iceberg_metadata_history`

Used by the replication service for polling only. No writes are performed against this
instance by the replication service.

### 4.3 Writable PostgreSQL (at Replica Site)

Owned and managed exclusively by the replication service. Contains:

- `iceberg_tables` — replica's own JDBC catalog; updated by the replication service as
  new metadata versions are successfully processed
- `iceberg_namespace_properties` — replica's namespace properties; periodically synced
  from the read-only replica by the replication service
- `replica_config` — replica ID, source/destination path prefixes, configurable parameters
- `failover_events` — recorded fail-over timestamps (used for fail-back reconciliation)
- `metadata_version_jobs` — one row per (replica_id, `iceberg_metadata_history` entry);
  tracks the processing state of each metadata version
- `file_rewrite_jobs` — distributed work queue; one row per file to be checked or rewritten
- `metadata_version_file_deps` — maps file jobs to metadata versions and tracks current snapshot status
- `file_job_children` — tracks parent-child relationships for file rewrites


## 5. Primary Catalog Enhancement

### 5.1 SQL Initialization Script

A self-contained SQL script, executed once on the **primary** PostgreSQL by the DBA at
replica setup time. Assumes `iceberg_tables` and `iceberg_namespace_properties` already
exist.

The script creates:

**`iceberg_metadata_history` table:**

| Column             | Type        | Description                                     |
|--------------------|-------------|-------------------------------------------------|
| `id`               | BIGSERIAL   | Surrogate primary key                           |
| `catalog_name`     | TEXT        | Iceberg catalog name                            |
| `table_namespace`  | TEXT        | Table namespace                                 |
| `table_name`       | TEXT        | Table name                                      |
| `metadata_location`| TEXT        | Absolute S3 path of the new metadata file (NULL for deletes) |
| `sequence_no`      | BIGINT      | Monotonically increasing per (catalog, namespace, table) |
| `recorded_at`      | TIMESTAMPTZ | Wall-clock time of the trigger firing (default NOW()) |

Index on `(catalog_name, table_namespace, table_name, sequence_no DESC)` for efficient
latest-first polling.

**Trigger:**

```sql
CREATE TRIGGER iceberg_metadata_history_trigger
AFTER INSERT OR UPDATE OF metadata_location OR DELETE ON iceberg_tables
FOR EACH ROW EXECUTE FUNCTION record_metadata_history();
```
*Note: For `DELETE` operations (e.g., dropped tables), the trigger inserts a tombstone marker (`metadata_location = NULL`) into `iceberg_metadata_history` to instruct the replica to drop the table from its writable catalog.*

**Retention:** Rows older than 7 days are eligible for deletion. Cleanup is implemented
as a periodic DELETE (via `pg_cron` or a scheduled DBA job) and is not managed by the
replication service itself. The 7-day window is chosen to comfortably exceed the maximum
expected S3 replication lag plus any operational downtime.

### 5.2 Rationale

The standard Iceberg JDBC catalog stores only the current (and sometimes previous) metadata
file pointer. For frequently-written tables, both entries may point to files not yet present
in the replicated S3 location. The history table extends this availability window to cover
worst-case replication lag with substantial headroom, at low storage cost (two text columns
per row, low write rate relative to data).


### 5.3 Replica Instance SQL Initialization Script

A self-contained SQL script executed by the `init-replica` command on the **replica
writable** PostgreSQL instance at setup time. Creates all tables required by the
replication service, including the replica's own Iceberg JDBC catalog tables.

#### Standard Iceberg JDBC Catalog Tables

The standard Iceberg JDBC catalog schema is created as-is, identical to the primary:

```sql
CREATE TABLE iceberg_tables (
    catalog_name                TEXT    NOT NULL,
    table_namespace             TEXT    NOT NULL,
    table_name                  TEXT    NOT NULL,
    metadata_location           TEXT,
    previous_metadata_location  TEXT,
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE iceberg_namespace_properties (
    catalog_name    TEXT    NOT NULL,
    namespace       TEXT    NOT NULL,
    property_key    TEXT    NOT NULL,
    property_value  TEXT,
    PRIMARY KEY (catalog_name, namespace, property_key)
);
```

These are managed exclusively by the replication service (writes the current rewritten
metadata pointer into `iceberg_tables`; syncs namespace properties from the read-only
replica periodically). No Iceberg writer process touches these tables directly on the
replica.

#### `replica_config` Table

Single-row table (one row per replica instance), populated by `init-replica` from
command-line arguments and configuration file:

| Column                      | Type        | Description                                                        |
|-----------------------------|-------------|--------------------------------------------------------------------|
| `replica_id`                | UUID        | Unique replica identifier; primary key; generated at init time     |
| `catalog_name`              | TEXT        | Iceberg catalog name (same as primary)                             |
| `source_catalog_jdbc_url`   | TEXT        | JDBC URL of the read-only replica PostgreSQL                       |
| `primary_s3_prefix`         | TEXT        | S3 URL prefix of the primary location (embedded in replicated file content; used as substitution source when rewriting position delete `_path` values) |
| `replicated_s3_prefix`      | TEXT        | S3 URL prefix of the replicated location (substitution destination for position delete `_path` values; source for all `HeadObject` checks) |
| `write_metadata_path`       | TEXT        | Destination S3 prefix for rewritten metadata, manifest list, and manifest files |
| `write_data_path`           | TEXT        | Destination S3 prefix for rewritten position delete files          |
| `s3_replication_sla_seconds`| INT         | Expected maximum S3 replication lag in seconds (default: 900); used as an operational reference only — not directly used in the `PERMANENTLY_FAILED` condition |
| `max_retry_attempts`        | INT         | Maximum S3 check retry attempts (404 responses only) before `PERMANENTLY_FAILED` (default: 20). Manifest deferrals due to pending position deletes do **not** increment this counter |
| `permanently_failed_timeout_seconds` | INT | Wall-clock timeout after which a job is marked `PERMANENTLY_FAILED` regardless of attempt count (default: 14 400 — 4 hours). Acts as a safety net for anomalous cases; `max_retry_attempts` is the intended primary trigger. Default chosen to exceed total retry-exhaustion time: with defaults (20 attempts, 30 s initial backoff, 2× multiplier, 10 min cap) exhaustion takes ~165 minutes |
| `retry_initial_interval_ms` | INT         | Initial exponential backoff interval in milliseconds (default: 30 000) |
| `retry_max_interval_ms`     | INT         | Backoff interval cap in milliseconds (default: 600 000)            |
| `lease_timeout_seconds`     | INT         | Seconds before an abandoned claimed job is eligible for re-claim (default: 300) |
| `worker_threads`            | INT         | Thread pool size for this instance (default: number of CPU cores)  |
| `max_inflight_jobs_per_table`| INT        | Per-table cap on concurrently in-flight jobs; enforces fairness across tables (default: 10) |
| `created_at`                | TIMESTAMPTZ | Timestamp of `init-replica` execution                              |

#### `failover_events` Table

Records operator-declared fail-over events. Used by `rollback-primary` to identify
the snapshot rollback boundary.

| Column        | Type        | Description                                              |
|---------------|-------------|----------------------------------------------------------|
| `id`          | BIGSERIAL   | Surrogate primary key                                    |
| `replica_id`  | UUID        | FK to `replica_config.replica_id`                        |
| `failover_at` | TIMESTAMPTZ | The fail-over timestamp declared by the operator; used as the rollback boundary (minus clock-drift tolerance) |
| `declared_at` | TIMESTAMPTZ | Wall-clock time when `declare-failover` was run (default NOW()) |
| `notes`       | TEXT        | Optional operator notes (nullable)                       |

#### `metadata_version_jobs` Table

One row per entry in `iceberg_metadata_history` observed for this replica. Tracks
the lifecycle of each metadata version from discovery through to catalog application.

| Column               | Type        | Description                                                  |
|----------------------|-------------|--------------------------------------------------------------|
| `id`                 | BIGSERIAL   | Surrogate primary key                                        |
| `replica_id`         | UUID        | FK to `replica_config.replica_id`                            |
| `history_entry_id`   | BIGINT      | `iceberg_metadata_history.id` on the read-only replica (informational; not a FK — cross-database reference) |
| `catalog_name`       | TEXT        |                                                              |
| `table_namespace`    | TEXT        |                                                              |
| `table_name`         | TEXT        |                                                              |
| `metadata_location`  | TEXT        | S3 path of the metadata file (copied from `iceberg_metadata_history`) |
| `sequence_no`        | BIGINT      | Copied from `iceberg_metadata_history`; used for ordering    |
| `recorded_at`        | TIMESTAMPTZ | Copied from `iceberg_metadata_history`                       |
| `status`             | TEXT        | `PENDING` \| `IN_PROGRESS` \| `APPLIED` \| `SUPERSEDED`     |
| `created_at`         | TIMESTAMPTZ | When this row was inserted (default NOW())                   |
| `applied_at`         | TIMESTAMPTZ | When this version was written to `iceberg_tables` (nullable) |

**Status transitions:**

```
PENDING     — metadata file job enqueued; S3 check not yet succeeded
IN_PROGRESS — metadata file available; child file jobs enqueued; awaiting completion
APPLIED     — all critical-path file jobs resolved; version written to iceberg_tables
SUPERSEDED  — a newer version reached APPLIED first; remaining jobs marked SUPERSEDED
```

A version may reach `APPLIED` with some `PERMANENTLY_FAILED` jobs, provided those
jobs belong exclusively to files referenced by expired (non-current) snapshots and
the current snapshot's full dependency chain is resolved (Section 8.6).

#### `file_rewrite_jobs` Table

Schema defined in Section 7.1. Created by this script. The following indexes are
required for correct and efficient operation:

```sql
-- Enforce one job per file per replica
UNIQUE INDEX idx_frj_source_path ON file_rewrite_jobs (replica_id, source_path);

-- Primary job queue polling: claim next available job for this replica
CREATE INDEX idx_frj_queue
    ON file_rewrite_jobs (replica_id, status, next_attempt_at)
    WHERE status = 'PENDING';

-- METADATA job priority: claim metadata file jobs ahead of child jobs
CREATE INDEX idx_frj_metadata_priority
    ON file_rewrite_jobs (replica_id, next_attempt_at)
    WHERE status = 'PENDING' AND file_type = 'METADATA';

-- Lease expiry sweep: find abandoned claimed jobs eligible for re-claim
CREATE INDEX idx_frj_lease_expiry
    ON file_rewrite_jobs (replica_id, claimed_at)
    WHERE status = 'CLAIMED';
```

#### `metadata_version_file_deps` Table

Schema defined in Section 7.2. Created by this script.

```sql
-- Completion check: find all jobs for a given version and their statuses
CREATE INDEX idx_mvfd_version
    ON metadata_version_file_deps (metadata_version_id);

-- Reverse lookup: find all versions that reference a given job
-- (used after a job reaches terminal state to trigger completion checks)
CREATE INDEX idx_mvfd_job
    ON metadata_version_file_deps (file_job_id);
```

#### `file_job_children` Table

Created by this script to record direct parent-to-child relationships for dependency cascading.

```sql
CREATE TABLE file_job_children (
    parent_job_id BIGINT NOT NULL,
    child_job_id  BIGINT NOT NULL,
    PRIMARY KEY (parent_job_id, child_job_id)
);
```

#### `metadata_version_jobs` Indexes

```sql
-- Latest-first discovery: find newest unseen history entries per table
CREATE INDEX idx_mvj_latest_first
    ON metadata_version_jobs (replica_id, catalog_name, table_namespace, table_name, sequence_no DESC);

-- Status sweep: find versions ready to apply or supersede
CREATE INDEX idx_mvj_status
    ON metadata_version_jobs (replica_id, status);
```


## 6. Replication Service: Core Loop

### 6.1 Main Loop

The service runs continuously until terminated (SIGTERM/SIGINT) and performs these
functions in a continuous loop across all tables in the catalog:

1. **Discovery:** Scan `iceberg_metadata_history` (read-only replica) for entries not yet
   recorded in `metadata_version_jobs`. For each new entry: insert a `metadata_version_jobs`
   row (status: PENDING) and immediately enqueue a `file_rewrite_jobs` entry of type METADATA
   for the metadata file. The metadata file path is known directly from
   `iceberg_metadata_history.metadata_location` — no S3 listing or scanning is required.
   *(Note: If `metadata_location` is NULL, indicating a dropped table, drop it from the replica catalog.)*
2. **File processing:** Worker threads claim and process `file_rewrite_jobs` via
   `SELECT ... FOR UPDATE SKIP LOCKED`. All file types are checked for S3 availability
   using `S3FileIO` and rewritten using the same job lifecycle (Section 7.4). All
   intermediate job types (METADATA, MANIFEST_LIST, MANIFEST) perform one-level child
   discovery (Section 7.5 step 3) from the parsed file content before marking themselves
   DONE. For MANIFEST jobs containing position deletes, child discovery always runs first;
   if those position delete children are not yet `DONE`, the manifest job is deferred
   (reset to `PENDING`) rather than written, and retried later.
3. **Catalog advancement:** A metadata version is considered **complete** when every row
   in `metadata_version_file_deps` for that version has a corresponding `file_rewrite_jobs`
   entry in a terminal state (`DONE`, `PERMANENTLY_FAILED`, or `SUPERSEDED`), subject to
   the constraint that no entry on the current snapshot's critical dependency path
   (where `is_current_snapshot = true`) may be `PERMANENTLY_FAILED` (see Section 8.6).

   - **Why the deps table is always fully populated before completion can succeed:** This
     protection operates at every level of the discovery chain. Each intermediate job
     (METADATA, MANIFEST_LIST, MANIFEST) is itself in `metadata_version_file_deps` as a
     non-terminal row. The completion check counts non-terminal rows, so it cannot return
     zero while any intermediate job is still `PENDING` or `CLAIMED`. Each intermediate job
     only reaches `DONE` (Section 7.5 step 4) after inserting all its direct children into
     `metadata_version_file_deps` (Section 7.5 step 3). Those children start as `PENDING`,
     which prevents completion from triggering prematurely. The guarantee propagates
     recursively: manifest list jobs insert manifest deps before reaching `DONE`; manifest
     jobs insert position delete deps before reaching `DONE`. At the moment the last
     position delete job reaches `DONE`, the completion check finds all deps terminal and
     the version is eligible for application.
   - **Detection:** after any worker transitions a job to a terminal state, it queries
     `metadata_version_file_deps` to find all metadata versions that reference that job and
     runs the completion check for each one. This is an O(versions per file) query,
     typically small. A background sweep additionally re-evaluates all `IN_PROGRESS`
     `metadata_version_jobs` rows periodically, as a safety net for workers that crash
     after marking a job terminal but before completing the version check.
   - **On completion:** the version is applied to `iceberg_tables` only if its
     `sequence_no` is greater than the currently-applied version's `sequence_no`. Older
     `IN_PROGRESS` versions with lower `sequence_no` are then marked `SUPERSEDED`; their
     `PENDING` file jobs that are exclusively referenced by `SUPERSEDED` versions (i.e.,
     no non-`SUPERSEDED` version depends on them via `metadata_version_file_deps`) are
     also marked `SUPERSEDED`.
4. **Lease recovery:** Reclaim `file_rewrite_jobs` whose `claimed_at` exceeds the
   configurable lease timeout (worker assumed crashed).

### 6.2 Latest-First Scheduling

The primary objective is minimising the lag to the most recent complete snapshot.

- `iceberg_metadata_history` is polled ordered by `sequence_no DESC` per table; a
  METADATA file job is enqueued immediately for each newly-seen entry (path is known
  from the history table — no S3 interaction at this stage).
- METADATA file jobs are given highest scheduling priority within the job queue, as
  their completion unblocks all child job discovery for that version.
- File jobs for newer metadata versions are prioritised over those for older versions.
- The most recently-completed metadata version remains applied to the replica catalog
  as a fallback while newer versions are still being processed.
- Once a newer version is successfully applied, older not-yet-applied `metadata_version_jobs`
  rows are marked SUPERSEDED. PENDING file jobs that are exclusively referenced by SUPERSEDED
  versions (no non-SUPERSEDED `metadata_version_jobs` references them via
  `metadata_version_file_deps`) are also marked SUPERSEDED. In-flight (CLAIMED) jobs are
  not interrupted; they will complete and their terminal state is harmless.

**Rationale for the inherent tension:** The most recent metadata version is the target,
but references the newest S3 files — which are least likely to have replicated yet. Older
versions, whose files have had more time to replicate, serve as a live fallback. This
approach minimises observable lag without sacrificing correctness.

**Design choice — older versions may be unavailable on the replica:** As a direct
consequence of latest-first scheduling, the replica may advance from an older applied
version directly to a much newer one, skipping intermediate versions entirely. Those
intermediate metadata versions are marked SUPERSEDED and their unprocessed jobs cancelled.
This means the replica will not have a complete Iceberg snapshot history: some historical
snapshots reachable via time-travel on the primary may not be accessible on the replica.
This is an intentional trade-off. Users of the replica are expected to care most about
the current (most recent) state of the table; minimising replication lag for that state
is prioritised over preserving full historical fidelity. Deployments that require complete
snapshot history on the replica must disable latest-first scheduling and process all
versions sequentially, accepting the increased lag this implies.

### 6.3 Threading Model

- Configurable thread pool size per service instance (default: number of available CPU cores)
- Per-table cap on concurrently in-flight jobs to prevent high-churn tables from starving
  others; all tables must maintain reasonably consistent replication lag
- Job selection: `SELECT ... FOR UPDATE SKIP LOCKED` ordered by
  `(table_namespace, table_name, sequence_no DESC)` within the per-table cap
- Multiple service instances are fully supported via the job queue coordination pattern

### 6.4 Resource Management

- **JDBC connections:** pooled and reused; separate pools for read-only replica and
  writable instance
- **Iceberg catalog instances:** one per catalog role (primary read-only, replica writable),
  shared across all threads
- **S3FileIO instances:** shared and reused across threads
- **No local disk:** all file reads and writes go directly through S3FileIO; no temporary
  files are created


## 7. Distributed Job Queue

### 7.1 `file_rewrite_jobs` Schema

One row per **unique file** per replica. A file that is referenced by multiple metadata
versions (e.g., a manifest reused across snapshots) has exactly one job row, shared by
all referencing versions. This is enforced by a UNIQUE constraint on `(replica_id, source_path)`.

| Column            | Type        | Description                                               |
|-------------------|-------------|-----------------------------------------------------------|
| `id`              | BIGSERIAL   | Surrogate primary key                                     |
| `replica_id`      | UUID        | Owning replica                                            |
| `source_path`     | TEXT        | Absolute S3 path at replicated location                   |
| `dest_path`       | TEXT        | Absolute S3 path at write location (deterministic prefix substitution) |
| `file_type`       | TEXT        | METADATA \| MANIFEST_LIST \| MANIFEST \| POSITION_DELETE |
| `status`          | TEXT        | PENDING \| CLAIMED \| DONE \| SUPERSEDED \| PERMANENTLY_FAILED |
| `claimed_by`      | UUID        | Instance UUID of claiming worker (nullable)               |
| `claimed_at`      | TIMESTAMPTZ | Lease start timestamp (nullable)                          |
| `attempts`        | INT         | Total attempt count (default 0)                           |
| `next_attempt_at` | TIMESTAMPTZ | Earliest time for next attempt (default NOW())            |
| `last_error`      | TEXT        | Last error message for operator inspection (nullable)     |

```sql
UNIQUE (replica_id, source_path)
```

When enqueueing a child file discovered from any intermediate file (METADATA, MANIFEST_LIST,
or MANIFEST job), use:

```sql
INSERT INTO file_rewrite_jobs (replica_id, source_path, dest_path, file_type, ...)
VALUES (...)
ON CONFLICT (replica_id, source_path) DO NOTHING;
```

Then unconditionally insert into `metadata_version_file_deps` (Section 7.2) using the
job's `id` — whether the INSERT above created a new row or hit the conflict. This ensures
every version correctly tracks all its file dependencies regardless of whether the
underlying job already existed.

### 7.2 `metadata_version_file_deps` Table

Junction table recording which file jobs each metadata version depends on. This is the
basis for both completion detection (Section 6.1 step 3) and SUPERSEDED propagation.

| Column                | Type    | Description                              |
|-----------------------|---------|------------------------------------------|
| `metadata_version_id` | BIGINT  | FK to `metadata_version_jobs.id`         |
| `file_job_id`         | BIGINT  | FK to `file_rewrite_jobs.id`             |
| `is_current_snapshot` | BOOLEAN | True if this file belongs to the version's current snapshot |

```sql
PRIMARY KEY (metadata_version_id, file_job_id)
```

Every file referenced by a metadata version — whether the `file_rewrite_jobs` row was
newly created or already existed from an earlier version — must have a row here. This
table is the authoritative source for "what does version X depend on?"

**Completion query** (run after any job reaches a terminal state):

```sql
SELECT COUNT(*) FROM metadata_version_file_deps mvfd
JOIN file_rewrite_jobs frj ON frj.id = mvfd.file_job_id
WHERE mvfd.metadata_version_id = ?
  AND frj.status NOT IN ('DONE', 'SUPERSEDED', 'PERMANENTLY_FAILED');
-- Result = 0 → all dependencies terminal; proceed to critical-path PERMANENTLY_FAILED check
```

**SUPERSEDED propagation query** (run after a version is marked SUPERSEDED):

```sql
UPDATE file_rewrite_jobs SET status = 'SUPERSEDED'
WHERE id IN (
    SELECT mvfd.file_job_id FROM metadata_version_file_deps mvfd
    WHERE mvfd.metadata_version_id = :superseded_version_id
    AND NOT EXISTS (
        SELECT 1 FROM metadata_version_file_deps mvfd2
        JOIN metadata_version_jobs mvj ON mvj.id = mvfd2.metadata_version_id
        WHERE mvfd2.file_job_id = mvfd.file_job_id
          AND mvfd2.metadata_version_id <> :superseded_version_id
          AND mvj.status NOT IN ('SUPERSEDED', 'APPLIED')
    )
)
AND status = 'PENDING';
-- Only PENDING jobs are superseded; CLAIMED jobs are left to complete harmlessly
```

### 7.3 `file_job_children` Table

Records direct parent-to-child relationships between file jobs, established during each
job's child discovery step. Used for cascading dep insertion when a new metadata version
references an intermediate job (METADATA, MANIFEST_LIST, or MANIFEST type) that was
already DONE for a previous version — meaning its children are already known and must
also be registered as deps for the new version.

| Column          | Type   | Description                                                         |
|-----------------|--------|---------------------------------------------------------------------|
| `parent_job_id` | BIGINT | FK to `file_rewrite_jobs.id` (METADATA, MANIFEST_LIST, or MANIFEST) |
| `child_job_id`  | BIGINT | FK to `file_rewrite_jobs.id`                                        |

```sql
PRIMARY KEY (parent_job_id, child_job_id)
```

A row is inserted during the parent job's discovery step (Section 7.5 step 3, substep b),
using `ON CONFLICT DO NOTHING`. Since Iceberg files are immutable, a file's children are
always the same regardless of which metadata version first discovered them. This makes the
table effectively append-only: rows are inserted once and never updated or deleted.

This table enables the recursive cascade query in Section 7.5 step 3 to insert all known
descendants of a DONE intermediate job into a new version's deps without re-reading any
files from S3.

### 7.4 Job Lifecycle

```
PENDING → CLAIMED → DONE
                  → PERMANENTLY_FAILED
                  → PENDING (manifest deferred: position delete children not yet DONE;
                             attempts counter unchanged; next_attempt_at set to short fixed delay)
         (timeout) → PENDING (re-claim after lease expiry)
PENDING → SUPERSEDED (no non-terminal version depends on this job)
```

Workers claim jobs atomically:

```sql
SELECT * FROM file_rewrite_jobs
WHERE replica_id = ? AND status = 'PENDING' AND next_attempt_at <= NOW()
ORDER BY /* priority */
FOR UPDATE SKIP LOCKED
LIMIT ?
```

### 7.5 File Availability and Retry

`S3FileIO` is the **sole mechanism** for checking file availability, applied
uniformly to all file types: METADATA, MANIFEST_LIST, MANIFEST, and POSITION_DELETE.
No S3 listing, scanning, or type-specific polling is performed. The `iceberg_metadata_history`
table (PostgreSQL) is the only polling target; metadata file paths come directly from it,
and all other file paths are discovered one level at a time as each intermediate job is
processed.

For each claimed job:

1. Issue an existence check against the **replicated** S3 location (`source_path`) using `S3FileIO`'s `newInputFile(path).exists()`. On `false`, go to step 5.
2. **All job types — fetch and parse:** Fetch the file from the replicated S3 location using `S3FileIO` and parse it in memory. The parsed content is held and reused for both child discovery (step 3) and the rewrite (step 4) without a second S3 read.
3. **METADATA, MANIFEST_LIST, and MANIFEST jobs only — child discovery:** Discover direct child file paths from the already-parsed source file content. Discovery is **one level deep only**, and cascades completion dependencies naturally:

   - **METADATA jobs:** collect all manifest list paths from the metadata file. The manifest list whose snapshot-id matches `current-snapshot-id` is enqueued with `is_current_snapshot = true`; every other manifest list (historical snapshots) is enqueued with `is_current_snapshot = false`. Only the current snapshot's dependency chain is on the critical path for version application.
   - **MANIFEST_LIST jobs:** collect manifest paths from the manifest list. Inherit the `is_current_snapshot` flag from the parent MANIFEST_LIST job.
   - **MANIFEST jobs:** collect position delete file paths from the manifest. Inherit the `is_current_snapshot` flag from the parent. (Data files and equality delete files require no jobs and are not enqueued.) After executing steps a–d for all discovered children, perform the position-delete readiness check described at the end of this step.
   - **POSITION_DELETE jobs:** no children; proceed directly to step 4.

   For each discovered child path, execute the following:

   **a.** Upsert the child job (resolve `id` via RETURNING or SELECT on the UNIQUE key):
   ```sql
   INSERT INTO file_rewrite_jobs(replica_id, source_path, dest_path, file_type, ...)
   VALUES (...) ON CONFLICT (replica_id, source_path) DO NOTHING
   ```

   **b.** Record the parent→child relationship:
   ```sql
   INSERT INTO file_job_children(parent_job_id, child_job_id) VALUES (?, ?) ON CONFLICT DO NOTHING
   ```

   **c.** Insert a dep row for every non-terminal metadata version that references
   the current (parent) job:
   ```sql
   INSERT INTO metadata_version_file_deps(metadata_version_id, file_job_id, is_current_snapshot)
   SELECT mvfd.metadata_version_id, :child_job_id, :is_current_snapshot
   FROM   metadata_version_file_deps mvfd
   JOIN   metadata_version_jobs mvj ON mvj.id = mvfd.metadata_version_id
   WHERE  mvfd.file_job_id = :parent_job_id
     AND  mvj.status NOT IN ('SUPERSEDED', 'APPLIED')
   ON CONFLICT (metadata_version_id, file_job_id)
   DO UPDATE SET is_current_snapshot = TRUE
   WHERE NOT metadata_version_file_deps.is_current_snapshot
   ```

   **d.** **If the child job is already in DONE state** (it was fully processed for a
   prior version), its own descendants are already recorded in `file_job_children`.
   Cascade them into deps for all non-terminal versions now referencing the child:
   ```sql
   WITH RECURSIVE descendants AS (
       SELECT child_job_id FROM file_job_children WHERE parent_job_id = :done_child_id
       UNION ALL
       SELECT fjc.child_job_id FROM file_job_children fjc
       JOIN   descendants d ON fjc.parent_job_id = d.child_job_id
   )
   INSERT INTO metadata_version_file_deps(metadata_version_id, file_job_id, is_current_snapshot)
   SELECT mvfd.metadata_version_id, d.child_job_id, :is_current_snapshot
   FROM   descendants d
   CROSS JOIN (
       SELECT mvfd2.metadata_version_id
       FROM   metadata_version_file_deps mvfd2
       JOIN   metadata_version_jobs mvj ON mvj.id = mvfd2.metadata_version_id
       WHERE  mvfd2.file_job_id = :done_child_id
         AND  mvj.status NOT IN ('SUPERSEDED', 'APPLIED')
   ) mvfd
   ON CONFLICT (metadata_version_id, file_job_id)
   DO UPDATE SET is_current_snapshot = TRUE
   WHERE NOT metadata_version_file_deps.is_current_snapshot
   ```

   **MANIFEST jobs — position-delete readiness check (runs after steps a–d for all children):**
   Query `file_rewrite_jobs` for the status of every position delete job enqueued in step a
   above (matching by `source_path`):
   - **All referenced position delete jobs are `DONE`:** proceed to step 4.
   - **Any referenced position delete job is not `DONE`:** defer this manifest job —
     reset `status = 'PENDING'`, clear `claimed_by` and `claimed_at`, set
     `next_attempt_at = NOW() + <short fixed delay>` (e.g. 5–10 seconds; independent of
     the exponential backoff schedule used for 404 retries). Do **not** increment
     `attempts`. Return without proceeding to step 4.

   **Crash safety:** if the worker crashes after child discovery (step 3) but before
   writing `dest_path` (step 4), the job remains `CLAIMED`; its lease expires and another
   worker re-runs from step 2. The existence check on `dest_path` in step 4 detects any
   already-written file and skips the write. The idempotent SQL in steps a–d
   (ON CONFLICT with conditional `is_current_snapshot` upsert) makes re-running
   discovery safe. For MANIFEST jobs that were reset to `PENDING` during the readiness
   check, the next worker picks them up cleanly via the normal claim query.

4. **All job types — write and complete:** Apply deterministic path substitution (Section 8)
   to the in-memory parsed content from step 2 and write the result to `dest_path` via
   `S3FileIO`. To ensure idempotency, first check whether `dest_path` already exists using
   `S3FileIO`'s `newInputFile(dest_path).exists()`; if it does, skip the write.
   For MANIFEST jobs whose position delete children were confirmed `DONE` in step 3,
   additionally fetch the new byte sizes of those rewritten position delete files via
   `S3FileIO` before writing the manifest, so that `file_size_in_bytes` entries are
   accurate per the Iceberg specification. Mark the job `DONE` and run the completion
   check (Section 6.1 step 3) for all metadata versions referencing this job via
   `metadata_version_file_deps`.
5. **On 404:** the file may not yet have replicated, or may have been garbage-collected on
   the primary before replication occurred. These are indistinguishable without primary S3
   access.
   - Increment `attempts`; compute `next_attempt_at` using exponential backoff
     (initial interval: `retry_initial_interval_ms`; multiplier: 2; cap: `retry_max_interval_ms`)
   - After `attempts` exceeds `max_retry_attempts` (default: 20) **or** when
     `(NOW() - job_created_at) > permanently_failed_timeout_seconds` (default: 14 400 s —
     4 hours): set status to `PERMANENTLY_FAILED` and record `last_error` for operator
     inspection. The timeout default is deliberately larger than total retry-exhaustion
     time (~165 min with defaults) so that `max_retry_attempts` is the primary trigger for
     files that will never arrive; the time threshold acts as a safety net for anomalous
     cases (e.g. a bug preventing `attempts` from incrementing).
   - Rationale: primary S3 is inaccessible from the replica service, so distinguishing
     "not yet replicated" from "permanently absent" requires both a retry budget and a
     wall-clock backstop.

### 7.6 Idempotency

Idempotency of file rewrites is required in **two distinct scenarios**, not just crash
recovery:

1. **Crash recovery:** a worker writes a file to `dest_path` then crashes before marking
   the job DONE. On restart, another worker claims the same job and finds `dest_path`
   already exists — it skips the write and marks the job DONE.

2. **Shared file reuse across metadata versions:** the same manifest file is referenced
   by multiple metadata versions. The `ON CONFLICT DO NOTHING` on `file_rewrite_jobs`
   ensures only one job row exists. Whichever worker first processes that job writes the
   file. All subsequent metadata versions that reference it via `metadata_version_file_deps`
   simply find the job already DONE — no second write occurs.

In both cases, the mechanism is identical: `dest_path` is derived deterministically from
`source_path` by prefix substitution (preserving relative path structure). Before writing,
the service checks whether `dest_path` already exists in S3. If so, the
write is skipped and the job is marked DONE immediately. Since the substitution is
deterministic and the source content is immutable (S3 replication does not modify file
content), both scenarios produce the same result whether the write happens once or is
skipped on a subsequent attempt.


## 8. File Rewriting

### 8.1 What Gets Rewritten and Where

| File Type             | Source Location      | Rewrite Required | Destination              |
|-----------------------|----------------------|------------------|--------------------------|
| Metadata files        | Replicated location  | Yes              | `write.metadata.path`    |
| Manifest list files   | Replicated location  | Yes              | `write.metadata.path`    |
| Manifest files        | Replicated location  | Yes (see §8.4)   | `write.metadata.path`    |
| Position delete files | Replicated location  | Yes (see §8.3)   | `write.data.path`        |
| Equality delete files | Replicated location  | No               | Used in-place            |
| Data files            | Replicated location  | No               | Used in-place            |

### 8.2 Rewrite Tool

Use `org.apache.iceberg.RewriteTablePathUtil` for metadata files, manifest lists, and as
the basis for manifest files. Implement only functionality not provided by this utility.

### 8.3 Position Delete File Rewriting

Position delete files contain a `_path` column (Parquet) with absolute data file paths
referencing the **primary** S3 location. These must be rewritten to reference data files
at the **replicated** S3 location (since data files are used in-place from there).

Implementation:
- Read source Parquet position delete file from replicated S3 location via S3FileIO
- Rewrite `_path` column values: substitute primary location prefix with replicated
  location prefix
- Write rewritten Parquet to `write.data.path` via S3FileIO
- No local disk; stream end-to-end through S3FileIO

`RewriteTablePathUtil` does not handle position delete file content rewriting. This is
custom logic implemented by this project.

### 8.4 Manifest File Rewriting

Manifests reference both data files and delete files. After rewriting, a manifest must
reference:
- **Data files** at the **replicated** S3 location (in-place, no copy)
- **Rewritten position delete files** at `write.data.path`
- **Equality delete files** at the replicated S3 location (in-place)

This mixed-destination reference pattern means `RewriteTablePathUtil`'s single-prefix
substitution is insufficient on its own. Manifest rewriting must apply per-entry path
substitution based on file type:
- Data file entries: substitute primary prefix → replicated prefix
- Position delete entries: substitute primary prefix → `write.data.path` prefix
- Equality delete entries: substitute primary prefix → replicated prefix

### 8.5 Rewriting Dependency Order and File Sizes

Path substitution for all file types is deterministic. Therefore, the physical destination paths (`dest_path`) can be calculated in-memory without waiting for child files to actually be written to S3. 

However, Iceberg manifest files strictly record the physical byte size (`file_size_in_bytes`) of the files they reference. Because rewriting a Parquet position delete file modifies its content (changing the `_path` values), its final byte size will change. To maintain strict Iceberg specification compliance, a manifest file cannot be rewritten until the new byte sizes of its referenced position delete files are known.

To optimize throughput, manifest file processing is strictly conditional based on its actual contents:

1. **Manifests without position deletes (Optimization Path):** The vast majority of manifests only contain data files or equality delete files (which are not rewritten). These manifest jobs can be processed immediately and asynchronously. The worker applies path substitutions in-memory, writes the new manifest to `write.metadata.path`, and marks the job `DONE` without waiting on any other jobs.
2. **Manifests with position deletes (Dependency Path):** When a worker claims a manifest job and discovers it contains position delete entries, it must ensure those specific files are fully rewritten to capture their new file sizes. The processing order is:
    * **Child discovery first (unconditional):** The worker executes §7.5 step 3 (steps a–d) for all referenced position delete paths. This inserts the position delete jobs into `file_rewrite_jobs` if they do not already exist (idempotent), registers the parent→child relationships, and propagates dep rows to all referencing metadata versions. This step always runs, regardless of whether the write proceeds.
    * **Readiness check:** The worker queries `file_rewrite_jobs` for the status of all referenced position delete jobs (now guaranteed to exist from the step above).
    * **If all referenced position delete jobs are `DONE`:** The worker fetches their new byte sizes (via `S3FileIO`), applies the deterministic path substitutions, writes the new manifest to `write.metadata.path`, and marks the job `DONE`.
    * **If any referenced position delete jobs are not `DONE`:** The worker resets the job to `PENDING` (`status = 'PENDING'`, `claimed_by` and `claimed_at` cleared, `next_attempt_at` set to a short fixed delay of 5–10 seconds). The `attempts` counter is **not** incremented — this is a routine scheduling deferral, not an S3 availability failure. The manifest is not written to `dest_path`.

This ensures that the dependency bottleneck is only incurred when absolutely necessary, allowing parallel processing for the rest of the metadata tree. No separate dependency-tracking table is required for this phase; the check is a targeted query against `file_rewrite_jobs` after child discovery ensures all rows exist.

### 8.6 Expired Snapshots

Expired snapshots and their associated files may be absent from the replicated S3 location
(garbage-collected on the primary before replication). The service must not fail on such
files. The retry-with-backoff and `PERMANENTLY_FAILED` state (Section 7.3) handles this
gracefully: files that will never arrive eventually reach the failure state for operator
review.

A metadata version whose non-expired files are all available is still processable even if
some files belonging to expired historical snapshots are permanently missing, provided the
**current** snapshot's file dependency chain is fully resolved.


## 9. Use Cases

### 9.1 Access Isolation (Unidirectional)

Replica provides isolated read-only access for testing, QA, or ad-hoc data exploration
without affecting the primary.

- Replication is strictly unidirectional (primary → replica only)
- The replica is never expected to become primary
- No fail-over configuration is required; `failover_events` table remains empty
- Setup and ongoing maintenance are simpler than the bidirectional case

### 9.2 Fail-Over (Bidirectional)

Replica serves as a hot standby in a secondary region. In normal operation it is not used
for writes. On fail-over it becomes the new primary.

**Normal Operation:**
- Site A (primary) replicates to Site B (replica)
- Site B's replication service processes arriving files and maintains a read-only replica
  catalog

**Fail-Over (operator-triggered):**
1. Operator records the fail-over event (timestamp) using the `declare-failover` CLI
   command (Section 10). This writes to `failover_events` on the replica's own writable
   PostgreSQL. If the primary site is reachable at the time of the command, it also
   attempts a best-effort write to the primary's `failover_events`; if the primary is
   unreachable (the common case during a fail-over), this attempt is silently skipped.
   All functional logic — including snapshot rollback boundary identification for
   fail-back — relies exclusively on the replica-side record.
2. Operator reconfigures S3 replication: Site B now replicates to Site A, with write
   location prefixes excluded from replication scope on both sides.
3. Site B operates as primary (writes go directly to its Iceberg catalog; replication
   service is stopped or reconfigured).
4. Site A's replication service begins processing files replicated from Site B.

**Fail-Back Recovery (operator-triggered, after partition heals):**

If Site A continued producing snapshots during the partition (after the fail-over
timestamp), those snapshots must be dropped before replication can resume cleanly.

1. Operator identifies the fail-over timestamp from `failover_events`
2. On Site A's primary Iceberg catalog, use `ManageSnapshots.rollbackTo(snapshotId)`,
   where `snapshotId` is the latest snapshot whose `timestamp-ms` is earlier than
   `(failover_timestamp - 60 seconds)`. The 60-second tolerance accommodates clock drift
   between sites. This is performed via the `rollback-primary` CLI command (Section 10),
   which requires write-capable JDBC access to Site A's primary PostgreSQL catalog.
   Because the replica service does not normally hold write credentials for the primary,
   `rollback-primary` must either (a) be executed directly on Site A using Site A's own
   catalog credentials, or (b) accept `--primary-jdbc-url` and credential arguments
   specifying a writable connection to Site A's catalog for use during fail-back only.
   The read-only replica connection used by the `status` command is insufficient for
   this operation.
3. Run `ExpireSnapshots` on Site A to clean up data files associated with rolled-back
   snapshots.
4. Operator reconfigures S3 replication back to the original direction (Site A → Site B),
   adjusting prefix scopes for the new write locations.
5. Normal replication resumes.

**Assumptions for fail-back:** Snapshot history on the primary after fail-over is assumed
to be linear (no branching or concurrent writers during the partition window). This is a
reasonable simplification given that prolonged primary operation post-fail-over is an
unlikely edge case.


## 10. Operator Tooling

The replication service is a single Picocli-based binary exposing the following subcommands:

| Subcommand          | Description                                                           |
|---------------------|-----------------------------------------------------------------------|
| `run`               | Start the continuous replication loop                                 |
| `init-replica`      | Initialise writable PostgreSQL schema; write replica configuration    |
| `declare-failover`  | Record a fail-over event with current (or specified) timestamp        |
| `rollback-primary`  | Run `ManageSnapshots.rollbackTo()` to last pre-failover snapshot on Site A's primary catalog. Requires write-capable JDBC access to Site A (see §9.2 fail-back) |
| `status`            | Report per-table replication lag, PERMANENTLY_FAILED job counts       |

The `PERMANENTLY_FAILED` job state is the primary alerting surface for operators. The
`status` command and direct database inspection of `file_rewrite_jobs` provide visibility
into stuck or failed replication work.


## 11. Implementation Technical Requirements

| Concern              | Choice                                                              |
|----------------------|---------------------------------------------------------------------|
| Language             | Java 17                                                             |
| Build tool           | Gradle                                                              |
| Framework            | None (no Spring Boot or equivalent)                                 |
| Iceberg version      | 1.10.1                                                              |
| CLI parsing          | Picocli                                                             |
| Database             | PostgreSQL 13+ (primary catalog and replica writable instance)      |
| S3 I/O               | Iceberg `S3FileIO`; no local disk; no temporary files               |
| JDBC connections     | Pooled and reused across threads                                    |
| Iceberg catalog      | One instance per catalog role, shared across threads                |
| S3FileIO             | One instance, shared across threads                                 |


## 12. Testing Requirements

### 12.1 Infrastructure

- Containerised services via a `docker-compose` project: Adobe s3mock (S3), PostgreSQL
  (×2: primary catalog, replica writable)
- **No TestContainers.** A Gradle task invokes shell scripts to start/stop the
  docker-compose project before and after the test suite.
- JUnit 5 only. No AssertJ, Mockito, or other testing libraries beyond JUnit.

### 12.2 Replication Simulation

Tests must simulate both replication mechanisms under controlled conditions:

- **S3 replication:** test fixtures manually copy files between s3mock buckets with
  configurable delay, allowing tests to verify behaviour during the replication lag window
- **PostgreSQL replication:** test fixtures manually execute SQL to sync rows from the
  primary PostgreSQL schema to the read-only replica schema (or separate database),
  with configurable delay

### 12.3 Required Test Coverage

Tests must cover at minimum:

- Normal replication: metadata version detected, files replicated, catalog advanced
- Replication lag: metadata pointer arrives before S3 files; service waits correctly
- Latest-first scheduling: newer version applied before older version completes
- Superseded version: older version jobs correctly skipped after newer version applied
- Expired snapshot: file never arrives; job reaches `PERMANENTLY_FAILED` after retry limit
- Idempotency: service restart mid-rewrite; correct resumption without duplicate files
- Multi-instance coordination: two concurrent service instances; no duplicate rewrites
- Fail-over / fail-back: rollback to pre-failover snapshot; correct snapshot boundary
  with clock drift tolerance
- Fairness: high-churn table does not starve low-churn table replication lag