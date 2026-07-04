# Apache Iceberg small-file compaction — design and implementation plan

## Executive summary

The table accumulates many small files per `(calendar_date, shard)` partition because writes arrive every few minutes, mostly as appends ordered by `calendar_time`, with occasional overwrites that add positional-delete files. This document describes a custom compaction planner — built on Iceberg's core/data Java API — that discovers fragmented partitions cheaply via metadata tables, selects the minimal set of files worth rewriting without breaking `calendar_time` ordering, splits that work into small balanced jobs (one output file each), and commits each job independently and atomically.

## 1. Problem

- Table partitioned by `calendar_date` (date, identity transform) and `shard` (int, identity transform).
- Continuous stream of updates, ordered by `calendar_time`, every few minutes — mostly appends, occasionally overwrites with positional deletes.
- Result: many small files per partition, which degrades query performance (file-open overhead, poor task parallelism, weak predicate pushdown).

## 2. Requirements

1. Prefer large file sizes, governed by `write.target-file-size-bytes`.
2. Never select a non-contiguous set of files to rewrite — no gaps in the `calendar_time` sequence.
3. Skip any `calendar_date` + `shard` partition with fewer than 8 files.
4. Plan small jobs, ideally one output file per job.
5. Process the most recent `calendar_date` first.
6. Use Iceberg metadata tables from the core API wherever possible.
7. Avoid rewriting already well-sized files repeatedly — pick the cheapest subset of files that gets a partition to 8 or fewer files.

## 3. Configuration knobs

| Knob | Purpose | Default |
|---|---|---|
| `write.target-file-size-bytes` | target output file size | table property, e.g. 512 MB |
| `smallFileThreshold` | below this, a file is a merge candidate | `0.8 × target` (hysteresis margin to avoid flapping) |
| `minFilesToTrigger` | skip partitions with fewer files than this | 8 |

## 4. Pipeline overview

1. **Partition discovery** — query the `PARTITIONS` metadata table for partitions with ≥ 8 files, sorted by `calendar_date` descending.
2. **File inventory** — query the `FILES` metadata table for each candidate partition: file size and `calendar_time` bounds.
3. **Select rewrite groups** — choose contiguous segments to merge, minimizing bytes touched, leaving well-sized files alone.
4. **Bin-pack into jobs** — split each segment into balanced, ~target-size jobs, one output file per job.
5. **Execute and commit** — read with deletes applied, write the new file, commit via `RewriteFiles`, independently per job.

## 5. Stage 1 — Partition discovery (`PARTITIONS` metadata table)

The `PARTITIONS` metadata table exposes per-partition `file_count` directly — no data files, and no `FILES`-table scan, are needed for this filter.

```java
Table table = catalog.loadTable(tableId);
Table partitionsTable = MetadataTableUtils.createMetadataTableInstance(
    table, MetadataTableType.PARTITIONS);

List<Record> candidates = new ArrayList<>();
try (CloseableIterable<Record> rows = IcebergGenerics.read(partitionsTable)
        .select("partition", "spec_id", "data_record_count", "data_file_count")
        .build()) {
  for (Record r : rows) {
    int fileCount = (int) r.getField("data_file_count");
    if (fileCount >= 8) candidates.add(r);
  }
}

candidates.sort(Comparator.comparing(
    (Record r) -> ((StructLike) r.getField("partition")).get(0, LocalDate.class))
    .reversed()); // calendar_date desc
```

This single scan satisfies requirements 3, 5, and 6 simultaneously.

## 6. Stage 2 — File inventory (`FILES` metadata table)

For each surviving partition, pull file size and `calendar_time` bounds without opening any data file:

```java
Table filesTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
int calTimeId = table.schema().findField("calendar_time").fieldId();

List<FileInfo> files = new ArrayList<>();
try (CloseableIterable<Record> rows = IcebergGenerics.read(filesTable)
        .select("content", "file_path", "file_size_in_bytes", "partition", "lower_bounds")
        .where(Expressions.and(
            Expressions.equal("partition.calendar_date", date),
            Expressions.equal("partition.shard", shard)))
        .build()) {
  for (Record r : rows) {
    if ((Integer) r.getField("content") != 0) continue; // data files only
    Map<Integer, ByteBuffer> lower = r.getField("lower_bounds");
    long calTime = (Long) Conversions.fromByteBuffer(
        table.schema().findField(calTimeId).type(), lower.get(calTimeId));
    files.add(new FileInfo(r.getField("file_path"), r.getField("file_size_in_bytes"), calTime));
  }
}
files.sort(Comparator.comparingLong(FileInfo::calTime));
```

This requires `calendar_time` to have column stats collected (the default, unless `write.metadata.metrics.column.calendar_time` is set to `none`). Delete files are handled separately in Stage 5.

## 7. Stage 3 — Selecting file groups to rewrite

### 7.1 Key insight

A contiguous segment of files merges into `ceil(sum(sizes) / target)` output files. If a segment already contains a file near `target`, adding it barely changes that ratio — you pay its full size in rewritten bytes for little or no file-count reduction. A sound selection rule never makes that trade, which means "avoid rewriting large files repeatedly" falls out of the cost function automatically rather than needing a separately tracked history.

### 7.2 Practical algorithm

```
target = write.target-file-size-bytes
smallThreshold = 0.8 * target

1. Classify each file SMALL (size < smallThreshold) or LARGE.
2. Find maximal contiguous runs of consecutive SMALL files (length >= 2).
3. For each run R: reduction(R) = len(R) - ceil(sum(R) / target).
4. Sort runs by reduction descending; greedily accept until
   (currentFileCount - cumulativeReduction) <= 8, or no positive-reduction runs remain.
5. Only if still > 8: consider "bridge" runs that include exactly one adjacent
   LARGE file. Accept the bridge with the best (reduction / large-bytes-touched)
   ratio, repeat until <= 8 or no beneficial bridge exists.
6. If 8 still isn't reachable without a zero-or-negative-benefit merge, stop —
   the partition's file count reflects legitimately well-sized data, not
   fragmentation, and isn't a performance problem.
```

### 7.3 Worked examples

- `[small1, small2, large1, small3]` — merging `[small1, small2]` costs only their bytes and reduces the count by one. Merging `[large1, small3]` instead would cost `large1`'s full size for `ceil((large1+small3)/target)`, almost always still 2 files — zero reduction for a large cost. `[small1, small2]` strictly dominates.
- `[small1, large1, small2, small3, large2, small4]` — the only segment with positive reduction-per-byte is `[small2, small3]`. Any segment dragging in `large1` or `large2` either doesn't reduce the count or costs far more per file saved. Both large files stay untouched.

### 7.4 Optional: provably-optimal selection (dynamic programming)

```
dp[i][k] = minimum bytes rewritten to reduce files[0..i) to exactly k output files,
           using only contiguous segmentations

cost(j, i)        = 1                                  if i - j == 1
                   = ceil(sum(files[j..i)) / target)    otherwise
rewriteBytes(j, i) = 0                                  if i - j == 1
                   = sum(files[j..i))                   otherwise

dp[0][0] = 0
dp[i][k] = min over j < i of: dp[j][k - cost(j, i)] + rewriteBytes(j, i)

answer = min(dp[n][1..8])   // if all infinite, take the smallest achievable k
```

`O(n² × 8)` per partition — trivial at the scale of dozens to low hundreds of files per partition. This reproduces the same decisions as the heuristic with a formal optimality guarantee, and needs no explicit small/large label — a segment that doesn't reduce `cost` relative to leaving its files separate is never selected.

### 7.5 Edge case

If a partition has many files that are already near `target` size, forcing the count down to 8 would mean merging well-sized files for no benefit. Step 6 of the heuristic (and the DP's fallback to "smallest achievable k") both protect against this — the 8-file figure is a fragmentation heuristic, not a hard cap.

## 8. Stage 4 — Bin-packing into balanced jobs

### 8.1 Why balance, not greedy-fill

Filling each output file up to `target` before starting the next one leaves whatever remains as the last file — frequently a near-target file followed by a much smaller one. Splitting the segment into evenly-sized files instead avoids that asymmetry, and avoids creating a fresh small file that would likely qualify as a merge candidate again on the very next compaction pass — undoing part of the work just done. This only changes *where* the cuts land; Stage 3's `cost(j,i) = ceil(sum/target)` already fixed *how many* files a segment becomes.

### 8.2 Adaptive balanced split (recommended)

```java
int n = (int) Math.ceil((double) segmentBytes / target);   // from Stage 3's cost()
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

Each `jobs` entry is one job producing one output file. Recomputing `idealSize` from the bytes and chunks *remaining* after each cut keeps the split adaptive to whatever size distribution actually occurred.

### 8.3 Optional: exact minimax balance (binary search)

For pathological size distributions where the heuristic's edges might matter, the classic "split into k contiguous parts minimizing the largest part" approach gives a provable guarantee:

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

One more linear pass with `cap = lo` reconstructs the cut points. `O(m log S)` — more machinery than the problem size usually warrants, but exact.

## 9. Stage 5 — Job execution and commit

```java
TableScan scan = table.newScan().filter(
    Expressions.and(Expressions.equal("calendar_date", date), Expressions.equal("shard", shard)));

Set<String> wanted = jobFiles.stream().map(FileInfo::path).collect(toSet());
List<FileScanTask> tasks = new ArrayList<>();
try (CloseableIterable<FileScanTask> all = scan.planFiles()) {
  for (FileScanTask t : all) if (wanted.contains(t.file().path().toString())) tasks.add(t);
}

DataWriter<Record> writer = Parquet.writeData(outputFile)
    .schema(table.schema()).withSpec(spec).withPartition(partitionData)
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .build();

for (FileScanTask task : tasks) {
  GenericDeleteFilter<Record> deletes =
      new GenericDeleteFilter<>(table.io(), task, table.schema(), table.schema());
  try (CloseableIterable<Record> rows = deletes.filter(openRows(task))) {
    for (Record r : rows) writer.write(r);
  }
}
writer.close();
DataFile newFile = writer.toDataFile();

RewriteFiles rewrite = table.newRewrite();
Set<DeleteFile> obsoleteDeletes = deletesFullyCoveredBy(tasks); // e.g. via DeleteFile.referencedDataFile()
rewrite.rewriteFiles(oldDataFiles, obsoleteDeletes, Set.of(newFile), Set.of());
rewrite.validateFromSnapshot(table.currentSnapshot().snapshotId());
try {
  rewrite.commit();
} catch (ValidationException e) {
  // a concurrent writer touched one of these files — drop the job;
  // the next planning pass re-derives everything from fresh metadata
}
```

`openRows` is a format-aware helper that opens the raw (pre-delete-filtering) rows from a data file using the table's `FileIO`. `GenericDeleteFilter` wraps its output and applies positional and equality deletes before any record reaches the writer.

```java
private CloseableIterable<Record> openRows(FileScanTask task) {
  DataFile file = task.file();
  InputFile inputFile = table.io().newInputFile(file.path().toString());
  Schema schema = table.schema();

  switch (file.format()) {
    case PARQUET:
      return Parquet.read(inputFile)
          .project(schema)
          .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
          .split(task.start(), task.length())
          .build();
    case ORC:
      return ORC.read(inputFile)
          .project(schema)
          .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
          .split(task.start(), task.length())
          .build();
    case AVRO:
      return Avro.read(inputFile)
          .project(schema)
          .createReaderFunc(DataReader::create)
          .split(task.start(), task.length())
          .build();
    default:
      throw new UnsupportedOperationException("Unsupported file format: " + file.format());
  }
}
```

Notes on `openRows`:
- **Split offsets.** `task.start()` and `task.length()` are passed through explicitly. For whole-file compaction tasks they will be `0` and `file.fileSizeInBytes()` respectively, but threading them through keeps the code correct if scan planning ever subdivides a file.
- **Full schema projection.** Passing `table.schema()` reads all columns, which is correct for compaction. A narrower `requestedSchema` could be threaded through if needed for future use cases such as sort-only rewrites.
- **Delete resolution.** `GenericDeleteFilter.filter()` receives the raw unfiltered `CloseableIterable<Record>` returned here and produces a delete-resolved stream — the result is what gets written to the new compacted file.

A positional-delete file whose `referencedDataFile()` matches one of the files in this job is fully absorbed once the rewrite completes (its target rows are simply missing from `newFile`), so it's removed in the same commit rather than left to linger.

## 10. Scheduling and idempotency

- Candidate partitions are processed in Stage 1's order (most recent `calendar_date` first); jobs within a partition are independent and can run on a bounded thread/executor pool.
- No persistent state is needed to avoid re-rewriting large files: every planning pass re-reads current file sizes from metadata, so a file just merged into a near-target output is classified LARGE on the next pass and excluded from future candidates, purely as a function of its current size.
- Because ingestion runs every few minutes, expect occasional commit conflicts. `validateFromSnapshot` plus catch-and-drop (rather than retry-forever) is the right failure mode — a dropped job is simply re-planned next cycle from current state.

## 11. Production integration note

If the execution layer is Spark, Iceberg's actions module already provides scaffolding for this kind of job: the `org.apache.iceberg.actions.RewriteStrategy` SPI behind `BinPackStrategy`/`SortStrategy`, including a commit-per-group manager and retry logic. Implementing a custom `RewriteStrategy` with Stage 3's `planFileGroups` logic and plugging it into `SparkActions.get().rewriteDataFiles(table)` reuses that infrastructure and only replaces the file-grouping decision. The fully custom core/data-API path in Stages 1–5 above is the right choice when not running on Spark (Flink, or a standalone Java service).

## 12. Requirements traceability

| Requirement | Mechanism |
|---|---|
| Respect `write.target-file-size-bytes` | drives `cost()` and the balanced split in Stages 3–4 |
| No ordering gaps | only contiguous segments are ever considered |
| Skip partitions with < 8 files | Stage 1 filter on the `PARTITIONS` table |
| Small jobs, ~1 output file each | Stage 4 balanced split |
| Most recent `calendar_date` first | Stage 1 sort |
| Use metadata tables | `PARTITIONS` + `FILES`, no data files opened for planning |
| Don't repeatedly rewrite large files | emergent from the cost function (greedy or DP), not a tracked flag |
