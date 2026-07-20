# iceberg-partition-compactor

A small Java library that plans compaction of small data files within one fixed "shard"
partition of an Apache Iceberg table, prioritizing the most recent dates, and cleans up position
delete files that become redundant as a result.

Design rationale, the file-selection algorithm (with worked examples), and exact API notes are in
[`docs/iceberg-compaction-plan-v2.md`](docs/iceberg-compaction-plan-v2.md) — read that first if
you're modifying this code or porting it to a different Iceberg version; it also lists exactly
which API details are worth re-verifying against source if so.

## What it does

Given an Iceberg `Table`, a timestamp column to order files by, the column that's the source of a
date partition field (`identity` or `day` transform), and a fixed `(shardColumn, shardValue)`,
`IcebergPartitionCompactor.plan()`:

1. finds every distinct partition tuple matching that shard value with enough data files to be
   worth compacting (most recent date first), via the `PARTITIONS` metadata table;
2. for each one, inventories its data files via the `FILES` metadata table;
3. selects contiguous, time-ordered runs of small files worth rewriting, leaving already
   well-sized files alone;
4. splits each run into one or more balanced, ~target-size jobs;
5. returns the jobs as `Runnable`s. **It does not run them.**

The caller submits the returned jobs to an executor and waits for them. Each job commits
independently via `RewriteFiles`; if it loses a commit race (`ValidationException`,
`CommitFailedException`, or `CommitStateUnknownException`), that's logged and the job simply
completes without throwing — it is *not* retried, on the assumption that a later compaction pass
will re-plan from whatever the table looks like by then. A job also removes any position delete
file that becomes fully redundant once its target data file is rewritten.

Any other columns in the partition spec besides the date and shard fields are preserved
automatically: files are only ever grouped within one exact, complete partition tuple.

## Requirements

- Java 17+ (matches Iceberg 1.11.0's own baseline)
- Gradle 7.4+ (for version catalog support: `gradle/libs.versions.toml`)

## Building

A Gradle wrapper is included, so no local Gradle install is required:

```
./gradlew build
```

(`gradlew.bat` on Windows.) This needs network access to Maven Central to resolve
Iceberg/Parquet/JUnit, and to `services.gradle.org` the first time, to download the Gradle
distribution the wrapper points at (8.12).

The implementation's logic was verified independently of a live Iceberg dependency (see "How this
was verified" below) — the environment it was written in has no route to either of those hosts, so
`./gradlew build` itself could not be run end-to-end there. Please run it as a first step and treat
any compile errors as a signal to check the specific API call named in the error against
[the re-verification checklist](docs/iceberg-compaction-plan-v2.md#13-re-verification-checklist-for-a-different-iceberg-version) —
that's exactly the kind of drift it's meant to catch.

## Usage

```java
Table table = catalog.loadTable(TableIdentifier.of("ns", "events"));

IcebergPartitionCompactor compactor = IcebergPartitionCompactor.builder(
        table,
        "event_time",     // timestamp column used to order files; need not be a partition field
        "event_date",     // source of the date partition field (identity date, or day(timestamp))
        "shard",          // source of the identity int shard partition field
        7)                // the one shard value to compact
    .minInputFiles(8)                       // default shown; skip partitions with fewer files
    .targetFileSizeBytes(512L << 20)        // default: table's write.target-file-size-bytes, or 512 MiB
    .smallFileThresholdRatio(0.8)           // default shown
    .build();

List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();

ExecutorService pool = Executors.newFixedThreadPool(8);
List<Future<?>> futures = jobs.stream().map(pool::submit).toList();
for (Future<?> f : futures) {
  f.get(); // a job completing normally covers both success and "dropped, will retry next cycle"
}
```

## Known limitations

See `docs/iceberg-compaction-plan-v2.md` §5 and §13 for the reasoning behind each of these:

- Only partitions written under the table's *current* partition spec are considered; data under
  a historical, evolved-away spec id is left alone.
- Only Parquet data files are read/written (kept dependencies minimal); a table using ORC or Avro
  data files will fail a job with `UnsupportedOperationException`.
- Only position delete files scoped to a single data file (`referencedDataFile() != null`) are
  ever removed, and only when that file is one of the ones being rewritten in the same job.
  Equality deletes are never touched.

## Testing

`IcebergPartitionCompactorTest` runs against `org.apache.iceberg.inmemory.InMemoryCatalog` — data
files and position delete files are actually written as Parquet and read back through the normal
Iceberg scan/read path, not mocked. Coverage includes: basic merge-and-preserve-rows behavior,
shard isolation, keeping other partition columns separate, most-recent-date-first ordering, the
`minInputFiles` threshold, leaving well-sized files alone, the `day`-transform date variant,
obsolete position-delete removal, and a simulated commit-conflict race.

## How this was verified

This was implemented in a sandboxed environment without access to Maven Central, so the Iceberg
API surface used here could not be compiled or run against the real `iceberg-*` jars directly.
Two things were done to compensate, and are worth knowing about if something doesn't compile:

1. Every non-trivial API call (metadata table column names, `RewriteFiles`'s current method
   surface, exception class hierarchy, delete-file writer API, etc.) was checked against
   Iceberg's actual 1.11.0 source and javadoc rather than taken from memory. Several
   discrepancies were found and fixed this way — they're called out explicitly in
   `docs/iceberg-compaction-plan-v2.md` §13 so they're easy to re-check for a different version.
2. The main class and test class were both compiled successfully with `javac` against a
   hand-written stub of the Iceberg/SLF4J/JUnit API surface they call, matching the signatures
   found in (1). This confirms the code is internally consistent (types, control flow, generics)
   but — unlike a real build — can't confirm the stubs themselves are accurate. The file-selection
   and bin-packing algorithm specifically (the trickiest, most bug-prone logic) was additionally
   extracted verbatim and run against the worked examples from the design doc plus several
   adversarial edge cases with plain `java`, independent of any Iceberg stub.
