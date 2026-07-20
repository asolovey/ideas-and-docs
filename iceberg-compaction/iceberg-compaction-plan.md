# Apache Iceberg partition compaction — design and implementation plan (v2)

## Revision note

This is a revision of an earlier draft, produced while actually implementing it against
Iceberg's real Java API (verified against the **1.11.0** source and javadoc; released May
2026, requires **Java 17+**). Two kinds of change were made:

- **Corrections.** Several API calls in the original draft do not match any real Iceberg
  release (wrong metadata-table column names, a deprecated commit method, a delete-filter
  class treated as generic when it isn't, `path()` used where `location()` is now preferred).
  These are fixed throughout, and called out explicitly in §13 so the mistake doesn't get
  copied again.
- **Generalization.** The original was written around one fixed example,
  `(calendar_date, shard)`. The real requirement is broader: the date partition column may be
  produced by either an `identity` or a `day` transform, the file-ordering column does not need
  to be a partition field at all, the shard column/value are runtime parameters rather than
  hardcoded names, and the partition spec may contain arbitrary additional ("other") columns
  that must be kept separate rather than merged across.

Everything algorithmic (file selection, worked examples, bin-packing math) is Iceberg-version
independent and should not need to change. Everything that names a class or method is
version-specific and should be re-verified against source before reuse on a different Iceberg
release — §13 gives a checklist and explains why each one is worth double-checking rather than
trusting from memory (an LLM's or a human's).

## 1. Problem

- A table is partitioned by, among possibly other unrelated columns:
  - a **date** partition field, produced by either an `identity` transform on a `date`-typed
    column, or a `day` transform on a `timestamp`/`timestamptz`-typed column, and
  - an **int**, identity-transformed "shard" partition field.
- A continuous stream of updates, ordered by some timestamp column (not necessarily the same
  column the date partition is derived from, and not necessarily a partition field itself at
  all), arrives every few minutes — mostly appends, occasionally overwrites that add positional
  delete files.
- Result: many small files per partition, which degrades query performance (file-open overhead,
  poor task parallelism, weak predicate pushdown).
- The compactor operates on **one shard value at a time** (a caller-supplied parameter), and
  must never merge files across different values of any other partition column even when they
  share the same date and shard.

## 2. Parameters

The compactor is configured with:

| Parameter | Meaning |
|---|---|
| `table` | the Iceberg table to compact |
| `orderingColumn` | timestamp-typed schema column used to order files; **may or may not** be part of the partition spec |
| `datePartitionSourceColumn` | schema column that is the *source* of the date partition field — either a `date` column under an `identity` transform, or a `timestamp`/`timestamptz` column under a `day` transform |
| `shardColumn`, `shardValue` | int schema column that is the source of an identity-transformed shard partition field, and the single value to compact |

Any other columns in the partition spec ("other" columns below) are left alone: the compactor
discovers and preserves every distinct combination of them automatically (§5), it never needs to
know their names up front.

## 3. Configuration knobs

| Knob | Purpose | Default |
|---|---|---|
| `write.target-file-size-bytes` | target output file size | table property; falls back to Iceberg's own default (512 MiB) if unset |
| `smallFileThreshold` | below this, a file is a merge candidate | `0.8 × target` (hysteresis margin to avoid flapping) |
| `minInputFiles` | skip partitions with fewer files than this | 8 |

All three should be overridable per call (not just via table properties) — tests in particular
need a way to force "always small" or "never small" deterministically without depending on the
exact byte size Parquet happens to encode a handful of rows as (§12).

## 4. Pipeline overview

1. **Partition discovery** — query the `PARTITIONS` metadata table for partitions matching the
   requested shard value with ≥ `minInputFiles` data files, sorted by date descending.
2. **File inventory** — query the `FILES` metadata table for each candidate partition's exact
   file set: size and ordering-column bounds.
3. **Select rewrite groups** — choose contiguous segments to merge, minimizing bytes touched,
   leaving well-sized files alone.
4. **Bin-pack into jobs** — split each segment into balanced, ~target-size jobs, one output file
   per job.
5. **Execute and commit** — read with deletes applied, write the new file, commit via
   `RewriteFiles`, independently per job, dropping (not retrying) on a commit conflict.

Planning (1–4) and execution (5) are separate: planning returns a list of `Runnable` jobs and
performs no writes; it is the caller's job to submit them to an executor and wait for the
futures. This matters because it means **execution can happen much later than planning**, against
whatever the table looks like by then — see §9's use of `useSnapshot`.

## 5. Parameter resolution — turning column names into partition fields

Given just `datePartitionSourceColumn` and `shardColumn` (schema column names), the compactor
must find the corresponding *partition fields*, which is not always the same name (a `day`
transform's default field name is derived from the source, e.g. `event_time_day`, not
`event_time`).

```java
Types.NestedField dateSource = schema.findField(datePartitionSourceColumnName);
PartitionField dateField = spec.fields().stream()
    .filter(pf -> pf.sourceId() == dateSource.fieldId())
    .filter(pf -> pf.transform().isIdentity() || "day".equals(pf.transform().toString()))
    .findFirst()
    .orElseThrow(...);

Types.NestedField shardSource = schema.findField(shardColumnName);
PartitionField shardField = spec.fields().stream()
    .filter(pf -> pf.sourceId() == shardSource.fieldId() && pf.transform().isIdentity())
    .findFirst()
    .orElseThrow(...);
```

`Transform#isIdentity()` is the robust way to detect an identity transform (rather than string
matching); comparing `transform().toString()` against `"day"` is how `day` is detected since
`Transform` has no `isDay()`. This is fine because a `day` transform's *result* type is itself
`date` (unlike `year`/`month`, whose results are plain integers) — so a date value read back from
a metadata table's partition struct is a `LocalDate` either way, regardless of whether it came
from an `identity` or a `day` field. This is what makes the rest of the pipeline able to treat
"the date partition value" uniformly without caring which transform produced it.

"Other" partition fields are simply `spec.fields()` minus `dateField` minus `shardField` — the
compactor never needs their names, because Stage 1 already enumerates one row per **complete**
partition tuple (§6), and Stage 2's filter is built from the complete tuple (§7), not just the
date and shard values. This is what keeps unrelated columns from ever being merged across.

**Locating a field's position inside a metadata table's partition struct.** `PARTITIONS` and
`FILES` expose "partition" as a `StructLike`, which only supports positional access
(`get(pos, Class)`), not by-name. It is tempting to assume position `i` in that struct matches
position `i` in `spec.fields()`, but that is only true for a table whose partition spec has never
evolved: `PARTITIONS`/`FILES` actually normalize every row's partition value into `
Partitioning.partitionType(table)` — the **union** of every historical spec's fields — so a
table with spec history can have extra or reordered fields relative to the *current* spec. The
robust lookup is by partition-field ID, not position number:

```java
Types.StructType unionType = Partitioning.partitionType(table);
int pos = IntStream.range(0, unionType.fields().size())
    .filter(i -> unionType.fields().get(i).fieldId() == targetField.fieldId())
    .findFirst()
    .orElseThrow(...);
```

**Scope decision:** this plan only compacts partitions written under the table's *current* spec
(`row.spec_id == table.spec().specId()`, from the `spec_id` column both metadata tables expose).
Data under a historical, evolved-away spec is left alone. This is a deliberate simplification,
not an oversight — handling spec evolution fully means re-implementing the kind of per-spec-id
field remapping `PartitionsTable` already does internally, which is a lot of machinery for a
maintenance job that can simply defer those (usually old, usually already-compacted) files to a
separate pass if it ever matters.

## 6. Stage 1 — Partition discovery (`PARTITIONS` metadata table)

```java
Table partitionsTable = MetadataTableUtils.createMetadataTableInstance(
    table, MetadataTableType.PARTITIONS);

List<PartitionCandidate> candidates = new ArrayList<>();
try (CloseableIterable<Record> rows = IcebergGenerics.read(partitionsTable)
        .select("partition", "spec_id", "file_count")
        .build()) {
  for (Record r : rows) {
    if ((int) r.getField("spec_id") != spec.specId()) continue; // current spec only, see §5
    int fileCount = (int) r.getField("file_count");
    if (fileCount < minInputFiles) continue;

    StructLike partition = (StructLike) r.getField("partition");
    Integer shard = partition.get(shardPos, Integer.class);
    if (shard == null || shard != shardValue) continue;

    LocalDate date = partition.get(datePos, LocalDate.class);
    candidates.add(new PartitionCandidate(copyAllFieldValues(partition, unionType), date, fileCount));
  }
}
candidates.sort(Comparator.comparing((PartitionCandidate c) -> c.date).reversed());
```

**Correction from v1:** the data-file count/record-count columns on `PARTITIONS` are named
`file_count` and `record_count`. (`data_file_count`/`data_record_count` — used in the original
draft — are not real column names in any released version; it's an easy name to guess wrong
because the table *also* has `position_delete_file_count`, `equality_delete_file_count`, and
similarly-prefixed columns, which invites a `data_` prefix that doesn't actually exist on the
data-file columns.) Always confirm metadata table schemas against the source
(`core/src/main/java/org/apache/iceberg/PartitionsTable.java`) rather than a remembered column
name — this table's schema has grown new columns across releases (delete counts,
`last_updated_at`, `last_updated_snapshot_id`), which is exactly the kind of drift that makes
half-remembered column names risky.

**Eagerly copy values out of the row.** `copyAllFieldValues` reads every field of the partition
struct into a plain `Map<String, Object>` (or similar) immediately, rather than retaining the
`StructLike`/`Record` reference past the end of the loop iteration. This is defensive: generic
readers are not guaranteed not to reuse row objects across iterations, and the cost of copying a
handful of scalar values per partition is negligible.

This single scan satisfies "skip small partitions", "most recent date first", and "use metadata
tables" simultaneously, generalized to filter on the *requested* shard value rather than a
hardcoded one.

## 7. Stage 2 — File inventory (`FILES` metadata table)

The v1 draft's filter (`partition.calendar_date = ? AND partition.shard = ?`) is only correct
when those are the *only* two partition fields. With arbitrary "other" columns present, the
filter must pin down the **entire** partition tuple, or files from two different, say, `region`
values sharing the same date and shard would be inventoried together:

```java
Expression filter = Expressions.alwaysTrue();
for (PartitionField pf : spec.fields()) {
  Object value = candidate.partitionValues.get(pf.name());
  filter = Expressions.and(filter,
      value == null ? Expressions.isNull("partition." + pf.name())
                     : Expressions.equal("partition." + pf.name(), value));
}
```

Because Stage 1 already captured every field's value for this exact candidate tuple, this filter
is built once per candidate with no extra table reads. Everything else is as in v1:

```java
Table filesTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
int orderingFieldId = schema.findField(orderingColumnName).fieldId();

List<FileInfo> files = new ArrayList<>();
try (CloseableIterable<Record> rows = IcebergGenerics.read(filesTable)
        .select("content", "file_path", "file_size_in_bytes", "lower_bounds")
        .where(filter)
        .build()) {
  for (Record r : rows) {
    if ((int) r.getField("content") != 0) continue; // 0 == DATA
    Map<Integer, ByteBuffer> lower = r.getField("lower_bounds");
    ByteBuffer bound = lower == null ? null : lower.get(orderingFieldId);
    if (bound == null) { /* no stats: see below */ continue; }
    long orderKey = ((Number) Conversions.fromByteBuffer(orderingFieldType, bound)).longValue();
    files.add(new FileInfo(r.getField("file_path").toString(), r.getField("file_size_in_bytes"), orderKey));
  }
}
files.sort(Comparator.comparingLong(FileInfo::orderKey));
```

This requires `orderingColumn` to have column stats collected (the default, unless
`write.metadata.metrics.column.<name>` is set to `none`). **If any file in the partition is
missing that stat, skip the whole partition** (log a warning) rather than guessing at its
position — a partial ordering is a correctness risk, not just a missed optimization. Delete files
are handled separately in Stage 9.

`file_path` (the metadata-table *column* name) and `location()` (the *method* name on a real
`DataFile`/`DeleteFile` object obtained from a scan) are two different things — see §13.

## 8. Stage 3 — Selecting file groups to rewrite

Unchanged from v1 — this stage is pure algorithm, no Iceberg API calls, and was re-verified
independently with a standalone (non-Iceberg) test harness while implementing this plan.

### 8.1 Key insight

A contiguous segment of files merges into `ceil(sum(sizes) / target)` output files. If a segment
already contains a file near `target`, adding it barely changes that ratio — you pay its full
size in rewritten bytes for little or no file-count reduction. A sound selection rule never makes
that trade, which means "avoid rewriting large files repeatedly" falls out of the cost function
automatically rather than needing a separately tracked history.

### 8.2 Practical algorithm

```
target = write.target-file-size-bytes
smallThreshold = 0.8 * target

1. Classify each file SMALL (size < smallThreshold) or LARGE.
2. Find maximal contiguous runs of consecutive SMALL files (length >= 2).
3. For each run R: reduction(R) = len(R) - ceil(sum(R) / target).
4. Sort runs by reduction descending; greedily accept until
   (currentFileCount - cumulativeReduction) <= minInputFiles, or no positive-reduction runs remain.
5. Only if still > minInputFiles: consider "bridge" runs - a leftover (still-unselected) maximal
   small run plus exactly one adjacent, unselected LARGE neighbor. Recompute candidates each
   round (accepting one bridge can invalidate another that wanted the same large neighbor);
   accept the best-scoring (reduction / large-file-bytes) beneficial bridge, repeat until
   <= minInputFiles or no beneficial bridge exists.
6. If minInputFiles still isn't reachable without a zero-or-negative-benefit merge, stop - the
   partition's file count reflects legitimately well-sized data, not fragmentation.
```

### 8.3 Worked examples

- `[small1, small2, large1, small3]` — merging `[small1, small2]` costs only their bytes and
  reduces the count by one. Merging `[large1, small3]` instead would cost `large1`'s full size
  for `ceil((large1+small3)/target)`, almost always still 2 files — zero reduction for a large
  cost. `[small1, small2]` strictly dominates.
- `[small1, large1, small2, small3, large2, small4]` — the only segment with positive
  reduction-per-byte is `[small2, small3]`. Any segment dragging in `large1` or `large2` either
  doesn't reduce the count or costs far more per file saved. Both large files stay untouched,
  and neither `small1` nor `small4` finds a beneficial bridge partner.

Both examples were checked mechanically (a small `main()`-driven harness reimplementing just this
stage, no Iceberg dependency) against the algorithm above; both produce exactly the segments
described.

### 8.4 Optional: provably-optimal selection (dynamic programming)

```
dp[i][k] = minimum bytes rewritten to reduce files[0..i) to exactly k output files,
           using only contiguous segmentations

cost(j, i)        = 1                                  if i - j == 1
                   = ceil(sum(files[j..i)) / target)    otherwise
rewriteBytes(j, i) = 0                                  if i - j == 1
                   = sum(files[j..i))                   otherwise

dp[0][0] = 0
dp[i][k] = min over j < i of: dp[j][k - cost(j, i)] + rewriteBytes(j, i)

answer = min(dp[n][1..minInputFiles])   // if all infinite, take the smallest achievable k
```

`O(n² × minInputFiles)` per partition — trivial at the scale of dozens to low hundreds of files
per partition. Not implemented in the reference implementation (the heuristic above already
matches its decisions on the worked examples and is simpler to reason about and maintain); listed
here in case a future need for a formal optimality guarantee arises.

### 8.5 Edge case

If a partition has many files that are already near `target` size, forcing the count down to
`minInputFiles` would mean merging well-sized files for no benefit. Step 6 of the heuristic (and
the DP's fallback to "smallest achievable k") both protect against this — the `minInputFiles`
figure is a fragmentation heuristic, not a hard cap.

## 9. Stage 4 — Bin-packing into balanced jobs

### 9.1 Why balance, not greedy-fill

Filling each output file up to `target` before starting the next one leaves whatever remains as
the last file — frequently a near-target file followed by a much smaller one. Splitting the
segment into evenly-sized files instead avoids that asymmetry, and avoids creating a fresh small
file that would likely qualify as a merge candidate again on the very next compaction pass —
undoing part of the work just done. This only changes *where* the cuts land; Stage 3's
`cost(j,i) = ceil(sum/target)` already fixed *how many* files a segment becomes.

### 9.2 Adaptive balanced split (recommended)

```java
long segmentBytes = segment.stream().mapToLong(FileInfo::size).sum();
// Capped at segment.size(): see "edge case" below.
int n = (int) Math.min(segment.size(), (segmentBytes + target - 1) / target);
if (n < 1) n = 1;

List<List<FileInfo>> jobs = new ArrayList<>();
long remainingBytes = segmentBytes;
int remainingChunks = n;
int idx = 0;
while (remainingChunks > 0) {
  long idealSize = remainingBytes / remainingChunks;  // recomputed from what's left
  List<FileInfo> chunk = new ArrayList<>();
  long chunkSum = 0;

  while (idx < segment.size()) {
    FileInfo f = segment.get(idx);
    int filesLeftAfterThis = segment.size() - idx - 1;
    int chunksLeftAfterThis = remainingChunks - 1;

    if (chunksLeftAfterThis > 0 && filesLeftAfterThis < chunksLeftAfterThis) break; // reserve files for remaining chunks
    if (!chunk.isEmpty() && chunksLeftAfterThis > 0
        && Math.abs(chunkSum + f.size() - idealSize) > Math.abs(chunkSum - idealSize)) {
      break; // adding f would overshoot the ideal more than stopping now undershoots it
    }
    chunk.add(f); chunkSum += f.size(); idx++;
  }
  jobs.add(chunk);
  remainingBytes -= chunkSum;
  remainingChunks--;
}
```

Each `jobs` entry is one job producing one output file. Recomputing `idealSize` from the bytes and
chunks *remaining* after each cut keeps the split adaptive to whatever size distribution actually
occurred.

**Edge case found while implementing this (not in v1): cap chunk count at file count.** A bridge
run (§8.2 step 5) can include a "large" file of *any* size above `smallThreshold` — nothing
bounds it above `target`. A pathologically oversized bridged file (say, 100× target, paired with
one small companion) makes `ceil(segmentBytes / target)` come out larger than the number of files
in the segment, and without the `Math.min(segment.size(), …)` cap shown above, the loop can be
asked to produce more chunks than there are files to put in them — the last chunk(s) would end up
empty. This was caught by an adversarial standalone test (many 1-byte files plus one huge file;
two files, one 100× target and one small) before it was ported into the real implementation, and
is worth keeping as a regression test regardless of Iceberg version since it's pure arithmetic.
With the cap in place, the invariant "every chunk gets ≥ 1 file" was proven to hold (by induction
on the `filesLeftAfterThis < chunksLeftAfterThis` guard, which is exactly what prevents any
chunk — including the first file it ever considers — from being closed out empty) and re-checked
against the adversarial cases.

### 9.3 Optional: exact minimax balance (binary search)

For pathological size distributions where the heuristic's edges might matter, the classic "split
into k contiguous parts minimizing the largest part" approach gives a provable guarantee:

```
canSplit(cap, files, n):
    chunks = 1; sum = 0
    for f in files:
        if sum + f.size > cap: chunks++; sum = f.size; if chunks > n: return false
        else: sum += f.size
    return true

lo = max(f.size for f in segment)
hi = segmentBytes
while lo < hi:
    mid = (lo + hi) / 2
    if canSplit(mid, segment, n): hi = mid else lo = mid + 1
// lo = minimal possible largest-chunk size, guaranteed <= target since n
// was already chosen as the minimum count for which that holds
```

One more linear pass with `cap = lo` reconstructs the cut points. `O(m log S)` — more machinery
than the problem size usually warrants, but exact. Not implemented in the reference
implementation, same rationale as §8.4.

## 10. Stage 5 — Job execution and commit

### 10.1 Re-scan at execution time, not planning time

Planning and execution are decoupled (§4): a job may run long after it was planned, against a
table that has since changed. Each job captures the snapshot ID current at *planning* time and
re-resolves its exact input files at *execution* time by scanning pinned to that snapshot,
filtered down for efficiency (not correctness — see below) and then matched against the exact set
of wanted file paths:

```java
Expression pruneFilter = Expressions.equal(shardColumnName, shardValue); // cheap, always identity
List<FileScanTask> tasks = new ArrayList<>();
try (CloseableIterable<FileScanTask> planned =
    table.newScan().useSnapshot(startingSnapshotId).filter(pruneFilter).planFiles()) {
  for (FileScanTask t : planned) {
    if (wantedPaths.contains(t.file().location())) tasks.add(t);
  }
}
if (tasks.size() != wantedPaths.size()) {
  // some file this job wanted is already gone at startingSnapshotId's ancestry as seen now, or
  // (more commonly) something has already changed this exact partition concurrently. Log and
  // bail out *before* doing any write - see §10.4, this is the same "don't retry" outcome as a
  // commit conflict, just detected earlier and more cheaply.
  return;
}
```

`pruneFilter` only narrows the scan for efficiency (shard is always identity-partitioned, so
Iceberg can push it down); it is never relied on for correctness. Correctness comes entirely from
the exact-path membership check against `wantedPaths`, which is why a mismatch there is treated
exactly like a commit conflict rather than attempted anyway.

### 10.2 Read, apply deletes, write

```java
DataWriter<Record> writer = Parquet.writeData(outputFile)
    .schema(table.schema()).withSpec(table.spec()).withPartition(outputPartition)
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .build();

for (FileScanTask task : tasks) { // in the same time-sorted order the job was planned in
  GenericDeleteFilter deleteFilter =
      new GenericDeleteFilter(table.io(), task, table.schema(), table.schema());
  try (CloseableIterable<Record> rows = deleteFilter.filter(openRows(task, deleteFilter.requiredSchema()))) {
    for (Record r : rows) writer.write(r);
  }
}
writer.close();
DataFile newFile = writer.toDataFile();
```

**Correction from v1: `GenericDeleteFilter` is not generic.** It's a concrete class,
`public class GenericDeleteFilter extends DeleteFilter<Record>` — construct and use it as
`GenericDeleteFilter`, not `GenericDeleteFilter<Record>`. The type parameter lives on the
*abstract* `DeleteFilter<T>` it extends, already fixed to `Record`.

**Read the delete filter's required schema, not just the table schema, for the raw open.**
`deleteFilter.requiredSchema()` is the schema to actually project when opening the raw file — it
is the requested schema plus whatever extra columns equality-delete matching needs. When (as
here) the requested schema is already the full table schema, the two coincide, but calling
`requiredSchema()` is the correct habit regardless of whether this particular use case happens to
make it a no-op.

**Output file construction.** Use `OutputFileFactory`, not a hand-built path — it handles unique
naming and encryption transparently:

```java
OutputFileFactory factory = OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PARQUET).build();
OutputFile outputFile = factory.newOutputFile(table.spec(), outputPartition).encryptingOutputFile();
```

(`outputPartition` here is any one of the job's `FileScanTask`s' `.file().partition()` — they're
all identical by construction, since Stage 2 already scoped every file to one exact partition
tuple.)

**Handle the "everything was already deleted" case.** If, after applying deletes, `newFile` ends
up with zero records (plausible: a burst of writes immediately followed by a burst of deletes,
all landing in files small enough to be selected together), don't add it — just delete the old
files (and their now-redundant delete files) and discard the empty physical file that was just
written, since it isn't going to be referenced by anything:

```java
boolean addingNewFile = newFile.recordCount() > 0;
if (!addingNewFile) table.io().deleteFile(newFile.location());
```

### 10.3 Commit

**Correction from v1: use the per-file `RewriteFiles` methods, not the grouped `rewriteFiles(...)`
overload.** The four-`Set`-argument `rewriteFiles(Set<DataFile>, Set<DeleteFile>, Set<DataFile>,
Set<DeleteFile>)` method the original draft used has been `@Deprecated` since 1.3.0 (still present
in 1.11.0, scheduled for removal in 2.0.0 per its javadoc, but there's no reason to write new code
against a method already marked for removal). The current, non-deprecated surface is individual
per-file calls:

```java
RewriteFiles rewrite = table.newRewrite();
if (addingNewFile) rewrite.addFile(newFile);
for (DataFile old : oldDataFiles) rewrite.deleteFile(old);
for (DeleteFile obsolete : obsoleteDeleteFiles) rewrite.deleteFile(obsolete);
rewrite.validateFromSnapshot(startingSnapshotId); // the snapshot captured at *planning* time
```

`oldDataFiles` and the `DataFile`/`DeleteFile` objects being deleted should come from the *fresh*
scan in §10.1, not from planning — they're the same underlying files (files are immutable), but
using the freshly-scanned objects avoids any risk of subtle identity/equality mismatches inside
`RewriteFiles`'s internal bookkeeping.

**On not needing to manage the new file's sequence number.** It's reasonable to worry that
assigning the compacted file a fresh (high) data sequence number could make it "invisible" to an
equality delete that logically should still apply. It doesn't, for this design specifically:
every currently-applicable delete (positional *and* equality) for each input file was already
resolved by `GenericDeleteFilter` while writing §10.2 — the new file's on-disk rows are already
the correct post-delete result. Any *new* delete added concurrently, after planning, is exactly
what `validateFromSnapshot` exists to catch (see below). `RewriteFiles.dataSequenceNumber(...)` is
available if a future variant of this job needs to control it explicitly, but is not needed here.

### 10.4 Commit conflicts — log, don't retry

```java
try {
  rewrite.commit();
} catch (ValidationException | CommitFailedException e) {
  // Expected, benign: somebody else committed a conflicting change first (most likely another
  // compaction pass, given how frequently this runs). Do not retry - a later pass will re-plan
  // from whatever the table looks like by then.
  if (addingNewFile) table.io().deleteFile(newFile.location()); // safe: this commit did NOT apply
} catch (CommitStateUnknownException e) {
  // The commit's outcome is genuinely unknown - it may have actually applied. Do not retry
  // (retrying risks double-applying a commit that already succeeded), but also do NOT delete
  // newFile here: unlike the case above, we cannot be sure it isn't now live.
}
```

**Correction/addition from v1: there are three exception types to consider, not one, and they are
three independent direct subclasses of `RuntimeException`** (`org.apache.iceberg.exceptions`) —
there is no common Iceberg-specific supertype to catch instead:

- `ValidationException` — `validateFromSnapshot`'s own conflict check failed (someone changed one
  of the files this job touches).
- `CommitFailedException` — the underlying catalog's compare-and-swap on the table's metadata
  pointer lost a race (after Iceberg's own internal retries, which handle pure contention, are
  exhausted).
- `CommitStateUnknownException` — genuinely ambiguous (e.g. a network timeout after the write
  landed server-side but before the client got confirmation). Its own javadoc says the client
  "cannot take any further action without possibly corrupting the table" — which is why it gets
  handled separately from the first two rather than folded into the same catch block: it's the
  one case where cleaning up the new file would itself be a risk, not a safety measure.

All three result in the job completing without throwing, per the "log and don't retry" contract —
letting any of them propagate out of a `Runnable` handed to an executor would surface as an
`ExecutionException` on the caller's `Future`, which invites exactly the retry behavior this
design wants to avoid.

`openRows` (format dispatch) is unchanged in spirit from v1 — open the raw, pre-delete-filtering
rows for a `FileScanTask` using the table's `FileIO`, dispatching on `file.format()` — except that
**a from-scratch implementation should decide up front how many formats to actually support**;
see §13 for why the reference implementation only wired up Parquet.

### 10.5 Removing obsolete position delete files

Unchanged in approach from v1, restated precisely: for each `FileScanTask` in the job, its
`.deletes()` already gives the exact delete files Iceberg's own scan planning has determined apply
to it (correctly accounting for sequence numbers — no manual correlation needed). A delete file is
safe to drop in this same commit if and only if it is a **position** delete
(`content() == FileContent.POSITION_DELETES`) that is scoped to a **single** data file
(`referencedDataFile() != null`) and that one file is among **this job's** old data files (not
just "somewhere in the partition" — a delete file scoped to a sibling file that isn't part of this
particular job must not be touched):

```java
for (FileScanTask task : tasks) {
  for (DeleteFile df : task.deletes()) {
    if (df.content() == FileContent.POSITION_DELETES
        && df.referencedDataFile() != null
        && df.referencedDataFile().equals(task.file().location())) {
      obsoleteDeleteFiles.add(df);
    }
  }
}
```

Equality deletes are never removed by this mechanism (and generally shouldn't be, by anything
short of a much more expensive table-wide liveness analysis — they aren't tied to one physical
file the way a scoped positional delete is). Note for newer Iceberg versions: format-v3 deletion
vectors are represented as `content() == POSITION_DELETES` with `referencedDataFile()` **always**
set (a deletion vector is inherently scoped to exactly one data file, more strictly than the
legacy positional-delete format where the field was optional) — so this logic requires no special
casing to also clean up deletion vectors once a table adopts them.

## 11. Scheduling and idempotency

- Candidate partitions are processed in Stage 1's order (most recent date first); jobs within and
  across partitions are independent (disjoint file sets) and can run concurrently on a bounded
  thread/executor pool.
- No persistent state is needed to avoid re-rewriting large files: every planning pass re-reads
  current file sizes from metadata, so a file just merged into a near-target output is classified
  LARGE on the next pass and excluded from future candidates, purely as a function of its current
  size.
- Because ingestion runs every few minutes, expect occasional commit conflicts.
  `validateFromSnapshot` plus catch-and-drop (§10.4, not retry-forever) is the right failure mode
  — a dropped job is simply re-planned next cycle from current state.

## 12. Testing strategy

Verified against `InMemoryCatalog` (`org.apache.iceberg.inmemory.InMemoryCatalog`, part of
`iceberg-core` since well before 1.11 — thread-safe, fully in-memory `Catalog` implementation
meant for exactly this kind of test): `new InMemoryCatalog(); catalog.initialize(name,
Map.of());`, then ordinary `Catalog` usage (`createNamespace`, `createTable`, `loadTable`).

A few things make integration tests against a real (if in-memory) table easier to write
deterministically:

- **Forcing "always small" or "never small" without knowing exact Parquet-encoded sizes.** Rather
  than trying to predict how many bytes a handful of test rows encode to, set `targetFileSizeBytes`
  absurdly large (e.g. 10 MB) to make every realistic test file "small" relative to it, or
  absurdly small (e.g. 10 bytes — smaller than any real Parquet file's footer) to make every file
  "large" and assert zero jobs get planned. Both are exact, not probabilistic.
- **`PartitionKey`** (`org.apache.iceberg.PartitionKey`, `new PartitionKey(spec, schema)` then
  `.partition(someRow)`) computes a correct partition tuple from a representative row by actually
  applying the spec's transforms — this is the right way to get a `StructLike` for
  `OutputFileFactory`/`Parquet.writeData(...).withPartition(...)` in test helpers, and avoids
  hand-computing a `day` transform's value.
- **Writing a position delete file for a test** uses a parallel builder to the data-file one:
  `Parquet.writeDeletes(outputFile).withSpec(spec).withPartition(partitionKey).buildPositionWriter()`,
  then `PositionDelete.create()`, `.set(dataFileLocation, position)` (deliberately not writing row
  data — see §13), `writer.write(positionDelete)`, then `writer.close()` /
  `writer.toDeleteFile()`, committed via `table.newRowDelta().addDeletes(deleteFile).commit()`.
- **Simulating a commit conflict**: plan a job, then — through a *second*, independently-loaded
  `Table` handle from the same catalog (simulating a genuinely separate writer) — commit a
  `RewriteFiles` that replaces the same input files before running the already-planned (now
  stale) job. Assert the stale job's `run()` does not throw, and that the table's current snapshot
  afterward is still the concurrent writer's, not the stale job's.
- Worth covering explicitly, beyond the obvious "rows are preserved and file count drops": the
  ordering column not being a partition field; the `day`-transform date variant; multiple values
  of an "other" partition column never getting merged together; the `minInputFiles` threshold; a
  fully-covered position delete file actually disappearing post-compaction along with its deleted
  row staying deleted.

## 13. Re-verification checklist for a different Iceberg version

Everything below was confirmed against 1.11.0 source/javadoc while writing this plan, specifically
*because* several of these turned out to be mis-remembered (or plausibly-hallucinated) in the v1
draft — this area of the API has real drift release to release, so treat any of the following as
worth a quick source check rather than trusting either version of this document from memory:

- **`PARTITIONS` metadata table columns.** Confirm the data-file count/record-count column names
  directly against `PartitionsTable.java` — this table has grown columns across releases
  (`position_delete_file_count` etc. were added after the base schema existed), which is exactly
  the situation that produces plausible-but-wrong guesses like `data_file_count`.
- **`RewriteFiles` method surface.** Confirm whether the grouped `rewriteFiles(Set, Set, Set,
  Set)` overload has been removed yet (it was `@Deprecated` targeting removal "in 2.0.0" as of
  1.11.0) and prefer the per-file `deleteFile`/`addFile` methods regardless.
- **`ContentFile#path()` vs `#location()`.** `path()` has been deprecated since 1.7.0 in favor of
  `location()` (which returns `String` instead of `CharSequence`); confirm it hasn't been removed
  outright. Note this is a *Java method* rename only — the `FILES`/`PARTITIONS` metadata table
  *column* is (still, as of 1.11.0) named `file_path` regardless of what the accessor method on a
  live `DataFile`/`DeleteFile` object is called.
- **`GenericDeleteFilter`'s generic-ness.** Confirm it's still a concrete, non-parameterized class
  over `Record` and hasn't itself become generic.
- **The "File Format API".** 1.11.0 introduced a new, unified `FormatModel`/format-registry API
  (`iceberg-core`/`iceberg-parquet`/`iceberg-orc` all gained "FormatModel" implementations) as an
  eventual replacement for the per-format `Parquet.write()`/`Parquet.writeData()`/`ORC.write()`
  static-builder style used throughout this plan. As of 1.11.0 the classic builders are still
  present and unremarked-as-deprecated; this plan deliberately keeps using them for stability and
  because they're far better documented. For a notably newer target version, check whether the
  classic builders have since been deprecated or removed in favor of the FormatModel API, and if
  so, port §10.2/§10.4's `openRows`/write-side code to it.
- **Position deletes "with row data".** Being deprecated in favor of deletion-vector-style
  (path+position only, no embedded row) deletes; this plan already only ever writes the
  row-data-free form in tests and never writes deletes at all in the compactor itself, so this is
  informational rather than something to fix, but it's worth confirming the row-data variant
  hasn't been removed outright if anything in a newer version needs to read one.
- **Format support scope.** The reference implementation only wires up Parquet read/write (not
  ORC/Avro), specifically to avoid pulling in `iceberg-orc`'s Hadoop-flavored transitive
  dependencies for a "minimum dependencies" requirement. If your target table can use ORC/Avro
  data files, port `openRows` to add those cases back (the v1 draft's `openRows` shows the shape:
  `ORC.read(...)`/`GenericOrcReader`, `Avro.read(...)`/`DataReader`), and expect to pull in the
  corresponding modules.
- **Java baseline.** 1.11.0 requires Java 17+ (Java 11 support was dropped). Older target versions
  may support Java 11; newer ones may raise the floor further — check before assuming 17 is right.

## 14. Production integration note

If the execution layer is Spark, Iceberg's actions module already provides scaffolding for this
kind of job: the `org.apache.iceberg.actions.RewriteStrategy` SPI behind
`BinPackStrategy`/`SortStrategy`, including a commit-per-group manager and retry logic.
Implementing a custom `RewriteStrategy` with Stage 3's `planFileGroups` logic and plugging it into
`SparkActions.get().rewriteDataFiles(table)` reuses that infrastructure and only replaces the
file-grouping decision. The fully custom core/data-API path in §5–§10 above is the right choice
when not running on Spark (Flink, or a standalone Java service) — which is what the accompanying
reference implementation targets.

## 15. Requirements traceability

| Requirement | Mechanism |
|---|---|
| Respect `write.target-file-size-bytes` | drives `cost()` and the balanced split in §8–§9 |
| No ordering gaps | only contiguous segments are ever considered |
| Skip partitions with < `minInputFiles` files | §6's `PARTITIONS`-table filter |
| Small jobs, ~1 output file each | §9's balanced split |
| Most recent date first | §6's sort |
| Use metadata tables | `PARTITIONS` + `FILES`, no data files opened for planning |
| Don't repeatedly rewrite large files | emergent from the cost function, not a tracked flag |
| Ordering column need not be a partition field | §5/§7 resolve it purely from the schema, independent of the spec |
| Date partition may be `identity` or `day` | §5's field lookup accepts either transform |
| Fixed shard partition, arbitrary other columns preserved | §5/§6/§7: full-tuple filters built from every spec field, not just date+shard |
| Remove obsolete position deletes | §10.5, scoped per-job via `referencedDataFile()` |
| Return runnable jobs; caller submits/waits | §4, §10 — planning performs no writes |
| Commit conflicts/`validateFromSnapshot` failures logged, not retried | §10.4, all three relevant exception types |
