package com.example.iceberg.compaction;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IcebergPartitionCompactor} against a real (in-memory) Iceberg table: data
 * files and position delete files are actually written as Parquet and read back through the
 * ordinary Iceberg scan/read path, exactly as they would be in production.
 */
class IcebergPartitionCompactorTest {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withoutZone()),
          Types.NestedField.required(3, "event_date", Types.DateType.get()),
          Types.NestedField.required(4, "shard", Types.IntegerType.get()),
          Types.NestedField.required(5, "region", Types.StringType.get()));

  private static final PartitionSpec IDENTITY_DATE_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("shard").identity("event_date").identity("region").build();

  private static final PartitionSpec DAY_TRANSFORM_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("shard").day("event_time").build();

  /** Deliberately tiny: even a single-row Parquet file exceeds this, so nothing is ever "small". */
  private static final long NO_MERGE_TARGET_BYTES = 10L;

  /** Deliberately huge: any test file is "small" relative to this. */
  private static final long ALWAYS_MERGE_TARGET_BYTES = 10_000_000L;

  private InMemoryCatalog catalog;
  private final AtomicInteger fileSeq = new AtomicInteger();

  @BeforeEach
  void setUp() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
  }

  // -----------------------------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------------------------

  @Test
  @DisplayName("a table with no snapshot yet plans no jobs")
  void emptyTable_returnsNoJobs() {
    Table table = createTable("empty_table", IDENTITY_DATE_SPEC);

    IcebergPartitionCompactor compactor =
        new IcebergPartitionCompactor(table, "event_time", "event_date", "shard", 1);

    assertTrue(compactor.plan().isEmpty());
  }

  @Test
  @DisplayName("merges small files in one partition, preserving every row and reducing file count")
  void mergesSmallFiles_preservingRows_reducingFileCount() throws IOException {
    Table table = createTable("merge_basic", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 15);

    List<Long> allIds = new ArrayList<>();
    List<DataFile> files = new ArrayList<>();
    long id = 0;
    for (int i = 0; i < 10; i++) {
      Record r1 = row(id, date.atTime(10, 0).plusMinutes(i * 2), date, 1, "us");
      Record r2 = row(id + 1, date.atTime(10, 0).plusMinutes(i * 2).plusSeconds(30), date, 1, "us");
      allIds.add(id);
      allIds.add(id + 1);
      id += 2;
      files.add(writeDataFile(table, List.of(r1, r2)));
    }
    append(table, files);
    assertEquals(10, countDataFiles(table));

    // orderingColumn (event_time) is deliberately NOT part of the partition spec here.
    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 1)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();

    List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();
    assertFalse(jobs.isEmpty());
    jobs.forEach(Runnable::run);

    assertTrue(countDataFiles(table) < 10, "file count should have gone down");
    List<Long> liveIds = liveIds(table);
    assertEquals(allIds.size(), liveIds.size(), "no rows should be lost or duplicated");
    assertEquals(new HashSet<>(allIds), new HashSet<>(liveIds));
  }

  @Test
  @DisplayName("only the requested shard is touched")
  void doesNotTouchOtherShards() throws IOException {
    Table table = createTable("shard_isolation", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 10);

    List<DataFile> shard1Files = new ArrayList<>();
    List<DataFile> shard2Files = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      shard1Files.add(writeDataFile(table, List.of(row(i, date.atTime(8, i), date, 1, "us"))));
      shard2Files.add(writeDataFile(table, List.of(row(100 + i, date.atTime(8, i), date, 2, "us"))));
    }
    append(table, shard1Files);
    append(table, shard2Files);
    assertEquals(12, countDataFiles(table));

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 1)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();
    compactor.plan().forEach(Runnable::run);

    assertEquals(6, countDataFilesForShard(table, 2), "shard 2 must be untouched");
    assertTrue(countDataFilesForShard(table, 1) < 6, "shard 1 should have been compacted");
  }

  @Test
  @DisplayName("files with the same date+shard but a different 'other' partition column are never mixed")
  void keepsOtherPartitionColumnsSeparate() throws IOException {
    Table table = createTable("region_isolation", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 12);

    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      files.add(writeDataFile(table, List.of(row(i, date.atTime(9, i), date, 5, "us"))));
      files.add(writeDataFile(table, List.of(row(200 + i, date.atTime(9, i), date, 5, "eu"))));
    }
    append(table, files);

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 5)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();
    List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();
    // us and eu are separate partition tuples, so they must plan as independent job(s).
    assertEquals(2, jobs.size());
    jobs.forEach(Runnable::run);

    for (Record r : readAll(table)) {
      String region = r.getField("region").toString();
      long rowId = (Long) r.getField("id");
      if (region.equals("us")) {
        assertTrue(rowId < 100, "a 'us' row must not have picked up a 'eu' id");
      } else {
        assertTrue(rowId >= 200, "a 'eu' row must not have picked up a 'us' id");
      }
    }
  }

  @Test
  @DisplayName("most recent date's jobs are planned first")
  void prioritizesMostRecentDateFirst() throws IOException {
    Table table = createTable("date_priority", IDENTITY_DATE_SPEC);
    LocalDate older = LocalDate.of(2026, 6, 1);
    LocalDate newer = LocalDate.of(2026, 7, 1);

    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      files.add(writeDataFile(table, List.of(row(i, older.atTime(9, i), older, 9, "us"))));
      files.add(writeDataFile(table, List.of(row(50 + i, newer.atTime(9, i), newer, 9, "us"))));
    }
    append(table, files);

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 9)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();
    List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();
    assertEquals(2, jobs.size(), "one job per date");

    int newerIndex = indexOfJobMentioning(jobs, newer.toString());
    int olderIndex = indexOfJobMentioning(jobs, older.toString());
    assertTrue(newerIndex >= 0 && olderIndex >= 0);
    assertTrue(newerIndex < olderIndex, "the more recent date's job should come first");
  }

  @Test
  @DisplayName("a partition with fewer than minInputFiles files is left alone")
  void respectsMinInputFilesThreshold() throws IOException {
    Table table = createTable("min_files", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 1);

    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      files.add(writeDataFile(table, List.of(row(i, date.atTime(9, i), date, 3, "us"))));
    }
    append(table, files);

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 3)
            .minInputFiles(8) // more files required than exist
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();

    assertTrue(compactor.plan().isEmpty());
    assertEquals(4, countDataFiles(table));
  }

  @Test
  @DisplayName("files that are already at/above the target size are never merged")
  void doesNotMergeAlreadyWellSizedFiles() throws IOException {
    Table table = createTable("well_sized", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 1);

    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      files.add(writeDataFile(table, List.of(row(i, date.atTime(9, i), date, 4, "us"))));
    }
    append(table, files);

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 4)
            .minInputFiles(3)
            .targetFileSizeBytes(NO_MERGE_TARGET_BYTES) // smaller than any real Parquet file
            .build();

    assertTrue(compactor.plan().isEmpty());
  }

  @Test
  @DisplayName("date partition derived via a day() transform on the ordering column works")
  void supportsDayTransformDatePartition() throws IOException {
    Table table = createTable("day_transform", DAY_TRANSFORM_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 8);

    List<Long> allIds = new ArrayList<>();
    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      allIds.add((long) i);
      files.add(writeDataFile(table, List.of(row(i, date.atTime(9, i), date, 6, "us"))));
    }
    append(table, files);

    // Here the ordering column IS the day-transform's source column - a common, realistic setup.
    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_time", "shard", 6)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();

    List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();
    assertFalse(jobs.isEmpty());
    jobs.forEach(Runnable::run);

    assertTrue(countDataFiles(table) < 8);
    assertEquals(new HashSet<>(allIds), new HashSet<>(liveIds(table)));
  }

  @Test
  @DisplayName("a position delete file fully covered by the rewrite is removed by the commit")
  void removesObsoletePositionDeleteFileAfterCompaction() throws IOException {
    Table table = createTable("delete_cleanup", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 5);

    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      files.add(writeDataFile(table, List.of(row(i, date.atTime(9, i), date, 2, "us"))));
    }
    append(table, files);

    // Delete row id=0, which lives at position 0 of files.get(0).
    DataFile targeted = files.get(0);
    PartitionKey pk = new PartitionKey(table.spec(), table.schema());
    pk.partition(row(0, date.atTime(9, 0), date, 2, "us"));
    DeleteFile deleteFile = writePositionDeleteFile(table, pk, targeted.location(), 0L);
    table.newRowDelta().addDeletes(deleteFile).commit();

    assertEquals(3, liveIds(table).size(), "row 0 should already read as deleted");
    assertEquals(1, countDeleteFilesReferenced(table));

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 2)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();
    compactor.plan().forEach(Runnable::run);

    List<Long> liveIds = liveIds(table);
    assertEquals(3, liveIds.size());
    assertFalse(liveIds.contains(0L), "the deleted row must still be gone after compaction");
    assertEquals(0, countDeleteFilesReferenced(table), "the now-redundant delete file must be removed");
  }

  @Test
  @DisplayName("a stale job that loses a commit race is logged and swallowed, not thrown")
  void staleJobCommitConflict_isSwallowed() throws IOException {
    Table table = createTable("commit_conflict", IDENTITY_DATE_SPEC);
    LocalDate date = LocalDate.of(2026, 7, 10);

    List<Record> rows = new ArrayList<>();
    List<DataFile> files = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Record r = row(i, date.atTime(9, i), date, 7, "us");
      rows.add(r);
      files.add(writeDataFile(table, List.of(r)));
    }
    append(table, files);

    IcebergPartitionCompactor compactor =
        IcebergPartitionCompactor.builder(table, "event_time", "event_date", "shard", 7)
            .minInputFiles(3)
            .targetFileSizeBytes(ALWAYS_MERGE_TARGET_BYTES)
            .build();
    List<IcebergPartitionCompactor.CompactionJob> jobs = compactor.plan();
    assertEquals(1, jobs.size());

    // Simulate a concurrent compaction winning the race on the very same files, through an
    // independent table handle (as a separate writer process would).
    Table concurrent = catalog.loadTable(TableIdentifier.of("ns", "commit_conflict"));
    DataFile winner = writeDataFile(concurrent, rows);
    RewriteFiles rewrite = concurrent.newRewrite();
    for (DataFile f : files) {
      rewrite.deleteFile(f);
    }
    rewrite.addFile(winner);
    rewrite.commit();
    long snapshotAfterWinner = concurrent.currentSnapshot().snapshotId();

    // The stale, already-planned job must not throw when it loses the race.
    assertDoesNotThrow(() -> jobs.get(0).run());

    Table reloaded = catalog.loadTable(TableIdentifier.of("ns", "commit_conflict"));
    assertEquals(
        snapshotAfterWinner,
        reloaded.currentSnapshot().snapshotId(),
        "the stale job must not have committed anything on top of the winner");
    assertEquals(4, liveIds(reloaded).size(), "no data lost or duplicated despite the conflict");
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  private Table createTable(String name, PartitionSpec spec) {
    return catalog.createTable(TableIdentifier.of("ns", name), SCHEMA, spec);
  }

  private static Record row(long id, LocalDateTime eventTime, LocalDate eventDate, int shard, String region) {
    Record r = GenericRecord.create(SCHEMA);
    r.setField("id", id);
    r.setField("event_time", eventTime);
    r.setField("event_date", eventDate);
    r.setField("shard", shard);
    r.setField("region", region);
    return r;
  }

  private DataFile writeDataFile(Table table, List<Record> rows) throws IOException {
    PartitionKey pk = new PartitionKey(table.spec(), table.schema());
    pk.partition(rows.get(0));

    OutputFileFactory outFactory =
        OutputFileFactory.builderFor(table, 0, fileSeq.incrementAndGet()).format(FileFormat.PARQUET).build();
    EncryptedOutputFile out = outFactory.newOutputFile(table.spec(), pk);

    // GenericFileWriterFactory (rather than Parquet.writeData(...) directly) avoids needing
    // org.apache.parquet.schema.MessageType on the compile classpath - see the main class's
    // Javadoc for why.
    FileWriterFactory<Record> writerFactory =
        GenericFileWriterFactory.builderFor(table).dataSchema(table.schema()).dataFileFormat(FileFormat.PARQUET).build();
    DataWriter<Record> writer = writerFactory.newDataWriter(out, table.spec(), pk);
    try {
      for (Record r : rows) {
        writer.write(r);
      }
    } finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  private DeleteFile writePositionDeleteFile(Table table, PartitionKey pk, String dataFileLocation, long... positions)
      throws IOException {
    OutputFileFactory outFactory =
        OutputFileFactory.builderFor(table, 0, fileSeq.incrementAndGet()).format(FileFormat.PARQUET).build();
    EncryptedOutputFile out = outFactory.newOutputFile(table.spec(), pk);

    FileWriterFactory<Record> writerFactory =
        GenericFileWriterFactory.builderFor(table)
            .dataSchema(table.schema())
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .build();
    PositionDeleteWriter<Record> writer = writerFactory.newPositionDeleteWriter(out, table.spec(), pk);
    try {
      PositionDelete<Record> positionDelete = PositionDelete.create();
      for (long pos : positions) {
        positionDelete.set(dataFileLocation, pos);
        writer.write(positionDelete);
      }
    } finally {
      writer.close();
    }
    return writer.toDeleteFile();
  }

  private static void append(Table table, List<DataFile> files) {
    AppendFiles append = table.newAppend();
    for (DataFile f : files) {
      append.appendFile(f);
    }
    append.commit();
  }

  private static int countDataFiles(Table table) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        count++;
      }
    }
    return count;
  }

  private static int countDataFilesForShard(Table table, int shard) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().filter(org.apache.iceberg.expressions.Expressions.equal("shard", shard)).planFiles()) {
      for (FileScanTask ignored : tasks) {
        count++;
      }
    }
    return count;
  }

  private static int countDeleteFilesReferenced(Table table) throws IOException {
    Set<String> deletePaths = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        for (DeleteFile df : t.deletes()) {
          deletePaths.add(df.location());
        }
      }
    }
    return deletePaths.size();
  }

  private static List<Record> readAll(Table table) {
    List<Record> rows = new ArrayList<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        rows.add(r);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private static List<Long> liveIds(Table table) {
    List<Long> ids = new ArrayList<>();
    for (Record r : readAll(table)) {
      ids.add((Long) r.getField("id"));
    }
    return ids;
  }

  private static int indexOfJobMentioning(List<IcebergPartitionCompactor.CompactionJob> jobs, String needle) {
    for (int i = 0; i < jobs.size(); i++) {
      if (jobs.get(i).toString().contains(needle)) {
        return i;
      }
    }
    return -1;
  }
}
