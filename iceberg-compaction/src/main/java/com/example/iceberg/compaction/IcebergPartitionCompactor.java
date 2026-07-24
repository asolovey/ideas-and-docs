package com.example.iceberg.compaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans compaction of small data files within one fixed "shard" partition of an Iceberg table,
 * prioritizing the most recent dates.
 *
 * <h2>What this does</h2>
 *
 * <p>The table is expected to be partitioned (among possibly other, unrelated columns) by:
 *
 * <ul>
 *   <li>a date-typed partition field, produced either by an {@code identity} transform on a
 *       {@code date} column or a {@code day} transform on a {@code timestamp}/{@code timestamptz}
 *       column ("date partition"), and
 *   <li>an {@code identity} transform on an {@code int} column ("shard partition").
 * </ul>
 *
 * <p>{@link #plan()} looks at every distinct partition combination (date x shard x any other
 * partition columns) that matches the requested shard value, and - for each one, most recent
 * date first - groups small data files into contiguous runs (ordered by a caller-supplied
 * timestamp column) and splits each run into one or more balanced, roughly target-sized
 * compaction jobs. Any position delete file that becomes fully redundant once its data file is
 * rewritten is removed as part of the same commit.
 *
 * <p>{@link #plan()} performs no writes. It returns a list of {@link CompactionJob}s; the caller
 * is responsible for submitting them to an executor and waiting for completion. Each job commits
 * independently. A commit conflict ({@link ValidationException} or {@link CommitFailedException})
 * is logged and swallowed rather than retried: the assumption is that a future compaction pass
 * will re-plan from the table's then-current state, so retrying stale work here would be wasted
 * effort at best and a source of duplicate work at worst.
 *
 * <h2>Known limitations</h2>
 *
 * <ul>
 *   <li>Only partitions written under the table's <em>current</em> partition spec are
 *       considered; data under a historical, evolved-away spec id is left alone. This is
 *       enforced twice: once when discovering candidate partitions (§ {@code spec_id} on the
 *       {@code PARTITIONS} table) and again, per file, immediately before it would be rewritten
 *       (§ {@code spec_id} on the {@code FILES} table and on each freshly-scanned {@link
 *       DataFile}) - the second check is what protects against the spec changing between
 *       planning and a job actually running.
 *   <li>Reading and writing goes through Iceberg's format registry ({@link
 *       FormatModelRegistry}, {@link GenericFileWriterFactory}) rather than calling {@code
 *       org.apache.iceberg.parquet.Parquet} directly, specifically so this class never needs
 *       {@code org.apache.parquet.schema.MessageType} (or any other Parquet-internal class) on
 *       its own compile classpath - only Parquet's own registered {@code FormatModel} does,
 *       inside {@code iceberg-parquet}, which this library only needs at <em>runtime</em>. A
 *       table using ORC or Avro data files will still cause a job touching those files to fail
 *       with {@link UnsupportedOperationException}, since only Parquet is registered as a
 *       runtime dependency here (see the Gradle build file to add another format).
 *   <li>Only <em>position</em> delete files that are scoped to a single data file (i.e.
 *       {@link DeleteFile#referencedDataFile()} is set) are ever removed, and only when that
 *       referenced data file is one of the ones being rewritten. Equality deletes are never
 *       touched.
 * </ul>
 *
 * <h2>Thread safety</h2>
 *
 * <p>A given {@link IcebergPartitionCompactor} instance may be reused to call {@link #plan()}
 * repeatedly. The returned {@link CompactionJob}s are independent of one another (each touches a
 * disjoint set of data files) and may be run concurrently.
 */
public final class IcebergPartitionCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergPartitionCompactor.class);

  /** Default minimum number of data files a partition must have before it is considered. */
  public static final int DEFAULT_MIN_INPUT_FILES = 8;

  /** Default fraction of the target file size below which a file is considered "small". */
  public static final double DEFAULT_SMALL_FILE_THRESHOLD_RATIO = 0.8;

  private final Table table;
  private final String orderingColumnName;
  private final String datePartitionSourceColumnName;
  private final String shardColumnName;
  private final int shardValue;
  private final int minInputFiles;
  private final long targetFileSizeBytes;
  private final long smallFileThresholdBytes;

  // Resolved once at construction time so plan() can be called repeatedly without repeating
  // schema/spec introspection.
  private final int orderingFieldId;
  private final Type orderingFieldType;
  private final PartitionField dateField;
  private final PartitionField shardField;

  /**
   * Creates a compactor with default planning parameters (see {@link #DEFAULT_MIN_INPUT_FILES},
   * {@link #DEFAULT_SMALL_FILE_THRESHOLD_RATIO}, and the table's own {@code
   * write.target-file-size-bytes} property). Use {@link #builder} to override any of these.
   *
   * @param table the Iceberg table to compact
   * @param orderingColumnName timestamp-typed column used to order data files; it does not need
   *     to be part of the partition spec
   * @param datePartitionSourceColumnName the schema column that is the source of the date
   *     partition field (an identity-partitioned {@code date} column, or the source of a {@code
   *     day}-transform partition field)
   * @param shardColumnName the schema column that is the source of an identity-partitioned {@code
   *     int} "shard" field
   * @param shardValue the single shard value to compact
   */
  public IcebergPartitionCompactor(
      Table table,
      String orderingColumnName,
      String datePartitionSourceColumnName,
      String shardColumnName,
      int shardValue) {
    this(
        builder(table, orderingColumnName, datePartitionSourceColumnName, shardColumnName, shardValue));
  }

  private IcebergPartitionCompactor(Builder b) {
    this.table = Objects.requireNonNull(b.table, "table must not be null");
    this.orderingColumnName = requireNonBlank(b.orderingColumnName, "orderingColumnName");
    this.datePartitionSourceColumnName =
        requireNonBlank(b.datePartitionSourceColumnName, "datePartitionSourceColumnName");
    this.shardColumnName = requireNonBlank(b.shardColumnName, "shardColumnName");
    this.shardValue = b.shardValue;

    if (b.minInputFiles < 1) {
      throw new IllegalArgumentException("minInputFiles must be >= 1, was " + b.minInputFiles);
    }
    this.minInputFiles = b.minInputFiles;

    if (b.smallFileThresholdRatio <= 0 || b.smallFileThresholdRatio > 1) {
      throw new IllegalArgumentException(
          "smallFileThresholdRatio must be in (0, 1], was " + b.smallFileThresholdRatio);
    }

    long target =
        b.targetFileSizeBytes != null
            ? b.targetFileSizeBytes
            : PropertyUtil.propertyAsLong(
                table.properties(),
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    if (target <= 0) {
      throw new IllegalArgumentException("targetFileSizeBytes must be > 0, was " + target);
    }
    this.targetFileSizeBytes = target;
    this.smallFileThresholdBytes = (long) (target * b.smallFileThresholdRatio);

    Schema schema = table.schema();
    PartitionSpec spec = table.spec();

    Types.NestedField orderingField = requireField(schema, this.orderingColumnName);
    requireTimestampType(orderingField);
    this.orderingFieldId = orderingField.fieldId();
    this.orderingFieldType = orderingField.type();

    Types.NestedField dateSourceField = requireField(schema, this.datePartitionSourceColumnName);
    this.dateField = findDatePartitionField(spec, dateSourceField);

    Types.NestedField shardSourceField = requireField(schema, this.shardColumnName);
    requireIntegerType(shardSourceField);
    this.shardField = findIdentityPartitionField(spec, shardSourceField);

    LOG.info(
        "Configured compactor for table {}: ordering='{}', date source='{}' (partition field "
            + "'{}'), shard source='{}' (partition field '{}') = {}, minInputFiles={}, "
            + "targetFileSizeBytes={}, smallFileThresholdBytes={}",
        table.name(),
        orderingColumnName,
        datePartitionSourceColumnName,
        dateField.name(),
        shardColumnName,
        shardField.name(),
        shardValue,
        minInputFiles,
        targetFileSizeBytes,
        smallFileThresholdBytes);
  }

  public static Builder builder(
      Table table,
      String orderingColumnName,
      String datePartitionSourceColumnName,
      String shardColumnName,
      int shardValue) {
    return new Builder(table, orderingColumnName, datePartitionSourceColumnName, shardColumnName, shardValue);
  }

  /** Fluent builder for overriding {@link IcebergPartitionCompactor}'s optional parameters. */
  public static final class Builder {
    private final Table table;
    private final String orderingColumnName;
    private final String datePartitionSourceColumnName;
    private final String shardColumnName;
    private final int shardValue;
    private int minInputFiles = DEFAULT_MIN_INPUT_FILES;
    private Long targetFileSizeBytes; // null => derive from table properties
    private double smallFileThresholdRatio = DEFAULT_SMALL_FILE_THRESHOLD_RATIO;

    private Builder(
        Table table,
        String orderingColumnName,
        String datePartitionSourceColumnName,
        String shardColumnName,
        int shardValue) {
      this.table = table;
      this.orderingColumnName = orderingColumnName;
      this.datePartitionSourceColumnName = datePartitionSourceColumnName;
      this.shardColumnName = shardColumnName;
      this.shardValue = shardValue;
    }

    /** Minimum data-file count a partition must have to be planned at all. Default 8. */
    public Builder minInputFiles(int value) {
      this.minInputFiles = value;
      return this;
    }

    /**
     * Target output file size in bytes. Defaults to the table's {@code
     * write.target-file-size-bytes} property (512 MiB if unset).
     */
    public Builder targetFileSizeBytes(long value) {
      this.targetFileSizeBytes = value;
      return this;
    }

    /** Fraction of the target size below which a file counts as "small". Default 0.8. */
    public Builder smallFileThresholdRatio(double value) {
      this.smallFileThresholdRatio = value;
      return this;
    }

    public IcebergPartitionCompactor build() {
      return new IcebergPartitionCompactor(this);
    }
  }

  /**
   * A single, independently-committable unit of compaction work: merge an ordered set of small
   * data files (all belonging to one exact partition tuple) into one new data file, dropping any
   * position delete file that becomes fully redundant as a result.
   *
   * <p>Running a job performs its own read, write, and commit; it does not retry on commit
   * conflicts (see the class Javadoc).
   */
  public interface CompactionJob extends Runnable {}

  /**
   * Plans compaction jobs for the configured table, shard, and date-partition column.
   *
   * <p>This method only reads table metadata; it performs no writes and commits nothing. It may
   * be called again later (e.g. on a fresh planning cycle) to get a fresh plan reflecting the
   * table's current state.
   *
   * @return jobs to run, ordered with the most recent date's jobs first; may be empty
   */
  public List<CompactionJob> plan() {
    if (table.currentSnapshot() == null) {
      LOG.info("Table {} has no current snapshot; nothing to compact", table.name());
      return List.of();
    }
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    PartitionSpec spec = table.spec();

    List<PartitionCandidate> candidates = discoverCandidatePartitions(spec);
    LOG.info(
        "Table {}: found {} partition(s) with {}={} and >= {} data file(s), most recent date first",
        table.name(),
        candidates.size(),
        shardColumnName,
        shardValue,
        minInputFiles);

    List<CompactionJob> jobs = new ArrayList<>();
    for (PartitionCandidate candidate : candidates) {
      try {
        List<FileInfo> files = inventoryFiles(candidate, spec);
        if (files.size() < minInputFiles) {
          continue;
        }
        List<List<FileInfo>> segments =
            selectSegments(files, targetFileSizeBytes, smallFileThresholdBytes, minInputFiles);
        int jobsForPartition = 0;
        for (List<FileInfo> segment : segments) {
          for (List<FileInfo> jobFiles : splitIntoBalancedJobs(segment, targetFileSizeBytes)) {
            jobs.add(
                new CompactionJobImpl(
                    table, candidate.partitionValues, jobFiles, startingSnapshotId, shardColumnName, shardValue));
            jobsForPartition++;
          }
        }
        LOG.debug(
            "Partition {} (date={}, {} data files): planned {} job(s)",
            candidate.partitionValues,
            candidate.date,
            files.size(),
            jobsForPartition);
      } catch (RuntimeException e) {
        LOG.warn("Skipping partition {} after a planning failure: {}", candidate.partitionValues, e.toString(), e);
      }
    }

    LOG.info("Table {}: planned {} compaction job(s) across {} partition(s)", table.name(), jobs.size(), candidates.size());
    return jobs;
  }

  // ---------------------------------------------------------------------------------------
  // Stage 1: discover candidate partitions via the PARTITIONS metadata table.
  // ---------------------------------------------------------------------------------------

  private List<PartitionCandidate> discoverCandidatePartitions(PartitionSpec spec) {
    Table partitionsTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
    Types.StructType unionType = Partitioning.partitionType(table);
    int datePos = positionOf(unionType, dateField.fieldId());
    int shardPos = positionOf(unionType, shardField.fieldId());

    Schema metaSchema = partitionsTable.schema();
    Accessor<StructLike> partitionAccessor = accessorFor(metaSchema, "partition");
    Accessor<StructLike> specIdAccessor = accessorFor(metaSchema, "spec_id");
    Accessor<StructLike> fileCountAccessor = accessorFor(metaSchema, "file_count");

    List<PartitionCandidate> candidates = new ArrayList<>();
    scanMetadataRows(
        partitionsTable,
        Expressions.alwaysTrue(),
        row -> {
          int specId = ((Number) specIdAccessor.get(row)).intValue();
          if (specId != spec.specId()) {
            // Data written under a historical (evolved-away) spec is out of scope for this
            // compactor; see the class Javadoc.
            return;
          }
          int fileCount = ((Number) fileCountAccessor.get(row)).intValue();
          if (fileCount < minInputFiles) {
            return;
          }
          StructLike partition = (StructLike) partitionAccessor.get(row);
          Integer shard = extractInt(partition, shardPos);
          if (shard == null || shard.intValue() != shardValue) {
            return;
          }
          LocalDate date = extractDate(partition, datePos);
          if (date == null) {
            return;
          }
          candidates.add(new PartitionCandidate(copyPartitionValues(partition, unionType), date, fileCount));
        });

    candidates.sort(Comparator.comparing((PartitionCandidate c) -> c.date).reversed());
    return candidates;
  }

  /**
   * Scans a metadata table (e.g. {@code PARTITIONS}, {@code FILES}), invoking {@code rowConsumer}
   * once per row.
   *
   * <p>Metadata tables are not backed by physical Parquet/ORC/Avro files - their rows are
   * synthesized in memory - so they must be read via {@link TableScan#planFiles()}'s {@link
   * DataTask#rows()}, not {@code IcebergGenerics.read(...)}. The latter routes through the same
   * format registry used for real data files (see the class Javadoc's dependency note) and fails
   * with {@code IllegalArgumentException: Format model is not registered for format METADATA}
   * for exactly this reason.
   *
   * <p>Deliberately does not project columns via {@code .select(...)}: the {@link Accessor}s
   * callers use to read fields out of each row are resolved from the metadata table's full,
   * unprojected {@link Table#schema()}, and a projected scan reshapes rows to match a narrower
   * schema - reading them through accessors bound to field positions in the *unprojected* schema
   * then fails with an out-of-bounds error. Metadata tables are cheap enough (bounded by a
   * table's partition/file count, not its row count) that this isn't worth the complexity of
   * re-deriving accessors from the scan's own projected schema instead.
   */
  private static void scanMetadataRows(Table metadataTable, Expression filter, Consumer<StructLike> rowConsumer) {
    try (CloseableIterable<FileScanTask> tasks = metadataTable.newScan().filter(filter).planFiles()) {
      for (FileScanTask task : tasks) {
        if (!task.isDataTask()) {
          throw new IllegalStateException(
              "Expected a DataTask scanning metadata table " + metadataTable.name() + ", got " + task.getClass());
        }
        try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
          for (StructLike row : rows) {
            rowConsumer.accept(row);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading metadata table " + metadataTable.name(), e);
    }
  }

  private static Accessor<StructLike> accessorFor(Schema schema, String fieldName) {
    return schema.accessorForField(requireField(schema, fieldName).fieldId());
  }

  /**
   * Reads an integer-valued partition field defensively with respect to representation: a plain
   * {@code Integer} either way, but future-proofed the same way as {@link #extractDate} in case
   * that ever stops holding.
   */
  private static Integer extractInt(StructLike partition, int pos) {
    Object value = partition.get(pos, Object.class);
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    throw new IllegalStateException("Expected a numeric partition value, got " + value.getClass() + ": " + value);
  }

  /**
   * Reads a date-valued partition field defensively with respect to representation: Iceberg's
   * "generic" API surface (as used everywhere else in this class, e.g. reading rows through
   * {@code Record}) represents a date as {@link LocalDate}, but internal/transform-oriented
   * machinery represents it as an {@code Integer} epoch-day. Which one a given {@link StructLike}
   * actually holds isn't guaranteed from the call site alone, so this accepts either rather than
   * asserting one and risking an {@code IllegalStateException: Not an instance of...} at runtime.
   */
  private static LocalDate extractDate(StructLike partition, int pos) {
    Object value = partition.get(pos, Object.class);
    if (value == null) {
      return null;
    }
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof Number) {
      return LocalDate.ofEpochDay(((Number) value).longValue());
    }
    throw new IllegalStateException("Expected a date partition value, got " + value.getClass() + ": " + value);
  }

  private static Map<String, Object> copyPartitionValues(StructLike partition, Types.StructType unionType) {
    // Eagerly copies every field's value out of the metadata table's row so the result stays
    // valid after the row/iterator that produced it is closed or advanced.
    Map<String, Object> values = new LinkedHashMap<>();
    List<Types.NestedField> fields = unionType.fields();
    for (int i = 0; i < fields.size(); i++) {
      values.put(fields.get(i).name(), partition.get(i, Object.class));
    }
    return values;
  }

  private static final class PartitionCandidate {
    final Map<String, Object> partitionValues;
    final LocalDate date;
    final int fileCount;

    PartitionCandidate(Map<String, Object> partitionValues, LocalDate date, int fileCount) {
      this.partitionValues = partitionValues;
      this.date = date;
      this.fileCount = fileCount;
    }
  }

  // ---------------------------------------------------------------------------------------
  // Stage 2: inventory a candidate partition's data files via the FILES metadata table.
  // ---------------------------------------------------------------------------------------

  private List<FileInfo> inventoryFiles(PartitionCandidate candidate, PartitionSpec spec) {
    Table filesTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.FILES);
    Expression filter = buildPartitionTupleFilter(spec, candidate.partitionValues);

    Schema metaSchema = filesTable.schema();
    Accessor<StructLike> contentAccessor = accessorFor(metaSchema, "content");
    Accessor<StructLike> pathAccessor = accessorFor(metaSchema, "file_path");
    Accessor<StructLike> sizeAccessor = accessorFor(metaSchema, "file_size_in_bytes");
    Accessor<StructLike> lowerBoundsAccessor = accessorFor(metaSchema, "lower_bounds");
    Accessor<StructLike> specIdAccessor = accessorFor(metaSchema, "spec_id");

    List<FileInfo> files = new ArrayList<>();
    AtomicBoolean missingStats = new AtomicBoolean(false);
    int currentSpecId = spec.specId();
    scanMetadataRows(
        filesTable,
        filter,
        row -> {
          if (missingStats.get()) {
            return; // already found a problem; see the comment below on why this doesn't abort the scan
          }
          int content = ((Number) contentAccessor.get(row)).intValue();
          if (content != 0) { // 0 == DATA; see org.apache.iceberg.FileContent
            return;
          }
          // Defense in depth on top of Stage 1's PARTITIONS-row-level spec_id filter: skip any
          // individual file not written under the table's current spec, see class Javadoc.
          int fileSpecId = ((Number) specIdAccessor.get(row)).intValue();
          if (fileSpecId != currentSpecId) {
            return;
          }
          String path = pathAccessor.get(row).toString();
          long size = ((Number) sizeAccessor.get(row)).longValue();
          @SuppressWarnings("unchecked")
          Map<Integer, ByteBuffer> lowerBounds = (Map<Integer, ByteBuffer>) lowerBoundsAccessor.get(row);
          ByteBuffer bound = lowerBounds == null ? null : lowerBounds.get(orderingFieldId);
          if (bound == null) {
            missingStats.set(true);
            return;
          }
          files.add(new FileInfo(path, size, extractOrderKey(bound)));
        });

    if (missingStats.get()) {
      LOG.warn(
          "Skipping partition {}: at least one data file has no recorded lower bound for ordering "
              + "column '{}', so file ordering cannot be determined safely",
          candidate.partitionValues,
          orderingColumnName);
      return List.of();
    }

    files.sort(Comparator.comparingLong(FileInfo::orderKey));
    return files;
  }

  private long extractOrderKey(ByteBuffer lowerBound) {
    Object value = Conversions.fromByteBuffer(orderingFieldType, lowerBound);
    return ((Number) value).longValue();
  }

  private static Expression buildPartitionTupleFilter(PartitionSpec spec, Map<String, Object> values) {
    Expression filter = Expressions.alwaysTrue();
    for (PartitionField pf : spec.fields()) {
      Object value = values.get(pf.name());
      Expression fieldFilter =
          value == null
              ? Expressions.isNull("partition." + pf.name())
              : Expressions.equal("partition." + pf.name(), value);
      filter = Expressions.and(filter, fieldFilter);
    }
    return filter;
  }

  /** A data file reduced to what Stage 3/4 planning needs: path, size, and its sort key. */
  private record FileInfo(String path, long size, long orderKey) {}

  // ---------------------------------------------------------------------------------------
  // Stage 3: within one partition's time-sorted files, select contiguous runs of small files
  // worth rewriting.
  // ---------------------------------------------------------------------------------------

  private static long reduction(List<FileInfo> files, int start, int endExclusive, long target) {
    long sum = 0;
    for (int i = start; i < endExclusive; i++) {
      sum += files.get(i).size();
    }
    long outputs = (sum + target - 1) / target; // ceil
    return (endExclusive - start) - outputs;
  }

  /**
   * Selects contiguous runs of small files worth compacting from a partition's data files, sorted
   * by ordering key ascending.
   *
   * <p>First, every maximal run of two or more consecutive small files is scored by how many
   * fewer files it would become once bin-packed to {@code target}, and runs are accepted
   * greedily, best reduction first, until the partition's file count would drop to {@code
   * minInputFiles} or no run yields a positive reduction. Second, any file(s) still left over that
   * are small but isolated (so step one could not pair them with another small file) are
   * considered for a "bridge" merge with exactly one adjacent large file, accepting the
   * best-scoring beneficial bridge repeatedly until the same stopping condition holds. Files that
   * are already close to the target size are never forced into a merge.
   *
   * @return disjoint, time-ordered segments to rewrite; never overlapping, possibly empty
   */
  private static List<List<FileInfo>> selectSegments(
      List<FileInfo> sorted, long target, long smallThreshold, int minInputFiles) {
    int n = sorted.size();
    boolean[] small = new boolean[n];
    for (int i = 0; i < n; i++) {
      small[i] = sorted.get(i).size() < smallThreshold;
    }

    boolean[] selected = new boolean[n];
    long cumulativeReduction = 0;

    List<int[]> runs = new ArrayList<>();
    int i = 0;
    while (i < n) {
      if (!small[i]) {
        i++;
        continue;
      }
      int start = i;
      while (i < n && small[i]) {
        i++;
      }
      if (i - start >= 2) {
        runs.add(new int[] {start, i});
      }
    }
    runs.sort(
        (a, b) -> Long.compare(reduction(sorted, b[0], b[1], target), reduction(sorted, a[0], a[1], target)));

    for (int[] run : runs) {
      if (n - cumulativeReduction <= minInputFiles) {
        break;
      }
      long red = reduction(sorted, run[0], run[1], target);
      if (red <= 0) {
        break; // sorted descending: no positive-reduction run remains
      }
      for (int idx = run[0]; idx < run[1]; idx++) {
        selected[idx] = true;
      }
      cumulativeReduction += red;
    }

    // Bridge pass: pull in exactly one adjacent large neighbor for small files step one couldn't
    // use. Recomputed each round since accepting one bridge can invalidate another that wanted
    // the same large neighbor.
    while (n - cumulativeReduction > minInputFiles) {
      int[] bestBridge = null;
      double bestScore = 0;
      long bestReduction = 0;

      int j = 0;
      while (j < n) {
        if (selected[j] || !small[j]) {
          j++;
          continue;
        }
        int start = j;
        while (j < n && !selected[j] && small[j]) {
          j++;
        }
        int end = j;

        if (start > 0 && !selected[start - 1] && !small[start - 1]) {
          long red = reduction(sorted, start - 1, end, target);
          if (red > 0) {
            double score = (double) red / sorted.get(start - 1).size();
            if (score > bestScore) {
              bestScore = score;
              bestReduction = red;
              bestBridge = new int[] {start - 1, end};
            }
          }
        }
        if (end < n && !selected[end] && !small[end]) {
          long red = reduction(sorted, start, end + 1, target);
          if (red > 0) {
            double score = (double) red / sorted.get(end).size();
            if (score > bestScore) {
              bestScore = score;
              bestReduction = red;
              bestBridge = new int[] {start, end + 1};
            }
          }
        }
      }

      if (bestBridge == null) {
        break;
      }
      for (int idx = bestBridge[0]; idx < bestBridge[1]; idx++) {
        selected[idx] = true;
      }
      cumulativeReduction += bestReduction;
    }

    List<List<FileInfo>> segments = new ArrayList<>();
    int k = 0;
    while (k < n) {
      if (!selected[k]) {
        k++;
        continue;
      }
      int start = k;
      while (k < n && selected[k]) {
        k++;
      }
      segments.add(new ArrayList<>(sorted.subList(start, k)));
    }
    return segments;
  }

  // ---------------------------------------------------------------------------------------
  // Stage 4: split a selected segment into one or more balanced, ~target-sized jobs.
  // ---------------------------------------------------------------------------------------

  /**
   * Splits one selected segment into {@code ceil(segmentBytes / target)} jobs, choosing each
   * job's boundary to land as close to an equal share of the remaining bytes as possible while
   * reserving at least one file per remaining job. File order is preserved.
   */
  private static List<List<FileInfo>> splitIntoBalancedJobs(List<FileInfo> segment, long target) {
    long segmentBytes = 0;
    for (FileInfo f : segment) {
      segmentBytes += f.size();
    }
    // Cap at segment.size(): a pathologically oversized bridged "large" file could otherwise
    // push ceil(segmentBytes / target) above the number of files available, which would leave
    // some chunk with zero files.
    int chunks = (int) Math.min(segment.size(), (segmentBytes + target - 1) / target);
    if (chunks < 1) {
      chunks = 1;
    }

    List<List<FileInfo>> jobs = new ArrayList<>();
    long remainingBytes = segmentBytes;
    int remainingChunks = chunks;
    int idx = 0;
    while (remainingChunks > 0) {
      long idealSize = remainingBytes / remainingChunks;
      List<FileInfo> chunk = new ArrayList<>();
      long chunkSum = 0;
      while (idx < segment.size()) {
        FileInfo f = segment.get(idx);
        int filesLeftAfterThis = segment.size() - idx - 1;
        int chunksLeftAfterThis = remainingChunks - 1;
        if (chunksLeftAfterThis > 0 && filesLeftAfterThis < chunksLeftAfterThis) {
          break; // must leave enough files for the remaining chunks
        }
        if (!chunk.isEmpty()
            && chunksLeftAfterThis > 0
            && Math.abs(chunkSum + f.size() - idealSize) > Math.abs(chunkSum - idealSize)) {
          break; // adding this file would overshoot the ideal size more than stopping now
        }
        chunk.add(f);
        chunkSum += f.size();
        idx++;
      }
      jobs.add(chunk);
      remainingBytes -= chunkSum;
      remainingChunks--;
    }
    return jobs;
  }

  // ---------------------------------------------------------------------------------------
  // Stage 5: execute one compaction job (read + merge + write + commit).
  // ---------------------------------------------------------------------------------------

  private static final class CompactionJobImpl implements CompactionJob {
    private final Table table;
    private final Map<String, Object> partitionValues;
    private final List<FileInfo> jobFiles;
    private final long startingSnapshotId;
    private final String shardColumnName;
    private final int shardValue;

    CompactionJobImpl(
        Table table,
        Map<String, Object> partitionValues,
        List<FileInfo> jobFiles,
        long startingSnapshotId,
        String shardColumnName,
        int shardValue) {
      this.table = table;
      this.partitionValues = partitionValues;
      this.jobFiles = jobFiles;
      this.startingSnapshotId = startingSnapshotId;
      this.shardColumnName = shardColumnName;
      this.shardValue = shardValue;
    }

    @Override
    public String toString() {
      return "CompactionJob{partition="
          + partitionValues
          + ", files="
          + jobFiles.size()
          + ", startingSnapshotId="
          + startingSnapshotId
          + "}";
    }

    @Override
    public void run() {
      Set<String> wantedPaths = new HashSet<>();
      for (FileInfo f : jobFiles) {
        wantedPaths.add(f.path());
      }

      LOG.info(
          "Starting compaction job for partition {}: {} file(s)", partitionValues, jobFiles.size());

      List<FileScanTask> tasks = new ArrayList<>();
      int specMismatches = 0;
      try (CloseableIterable<FileScanTask> planned =
          table.newScan().useSnapshot(startingSnapshotId).filter(scanPruningFilter()).planFiles()) {
        int currentSpecId = table.spec().specId();
        for (FileScanTask t : planned) {
          if (!wantedPaths.contains(t.file().location())) {
            continue;
          }
          // Defense in depth on top of Stage 1's spec_id filter (which filters PARTITIONS rows,
          // not individual files): never rewrite a file that isn't under the table's *current*
          // spec, even if it slipped through planning or the spec changed since then. Treated
          // exactly like a missing file below - the job is dropped, not partially run.
          if (t.file().specId() != currentSpecId) {
            specMismatches++;
            continue;
          }
          tasks.add(t);
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed planning input files for compaction job", e);
      }

      if (tasks.size() != wantedPaths.size()) {
        LOG.warn(
            "Compaction job for partition {} skipped: expected {} file(s) but found {} at "
                + "snapshot {} ({} excluded for not matching the table's current partition spec). "
                + "Another writer likely already touched this partition, or the spec changed; it "
                + "will be re-planned on the next compaction pass.",
            partitionValues,
            wantedPaths.size(),
            tasks.size(),
            startingSnapshotId,
            specMismatches);
        return;
      }
      tasks = orderLikeJobFiles(tasks);

      Set<DataFile> oldDataFiles = new HashSet<>();
      Set<DeleteFile> obsoleteDeleteFiles = new HashSet<>();
      for (FileScanTask t : tasks) {
        oldDataFiles.add(t.file());
        for (DeleteFile df : t.deletes()) {
          if (df.content() == FileContent.POSITION_DELETES
              && df.referencedDataFile() != null
              && df.referencedDataFile().equals(t.file().location())) {
            obsoleteDeleteFiles.add(df);
          }
        }
      }

      StructLike outputPartition = tasks.get(0).file().partition();
      DataFile newDataFile;
      try {
        newDataFile = writeMergedDataFile(tasks, outputPartition);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed writing compacted data file", e);
      }

      boolean addingNewFile = newDataFile.recordCount() > 0;
      RewriteFiles rewrite = table.newRewrite();
      if (addingNewFile) {
        rewrite.addFile(newDataFile);
      } else {
        // Every row in this job's input files was already deleted; just drop the inputs (and
        // their now-redundant deletes) and discard the empty file we wrote instead of adding it.
        LOG.info(
            "Compaction job for partition {}: all rows were already deleted; removing {} input "
                + "file(s) without adding a replacement",
            partitionValues,
            oldDataFiles.size());
        safeDelete(newDataFile.location());
      }
      for (DataFile old : oldDataFiles) {
        rewrite.deleteFile(old);
      }
      for (DeleteFile obsolete : obsoleteDeleteFiles) {
        rewrite.deleteFile(obsolete);
      }
      rewrite.validateFromSnapshot(startingSnapshotId);

      try {
        rewrite.commit();
        LOG.info(
            "Compaction job for partition {} committed: replaced {} data file(s) and removed {} "
                + "obsolete position delete file(s){}",
            partitionValues,
            oldDataFiles.size(),
            obsoleteDeleteFiles.size(),
            addingNewFile ? "" : " (no replacement file added)");
      } catch (ValidationException | CommitFailedException e) {
        // Expected, benign race: somebody else committed a conflicting change first. Do not
        // retry - a later compaction pass will re-plan from whatever the table looks like then.
        LOG.warn(
            "Compaction job for partition {} ({} file(s)) hit a commit conflict and was dropped "
                + "without retrying: {}",
            partitionValues,
            jobFiles.size(),
            e.toString());
        if (addingNewFile) {
          safeDelete(newDataFile.location());
        }
      } catch (CommitStateUnknownException e) {
        // The commit may or may not have applied. Do not retry (retrying could double-apply a
        // commit that actually succeeded) and do not delete the new file (it may now be live).
        LOG.error(
            "Compaction job for partition {} ({} file(s)): commit outcome is UNKNOWN. Not "
                + "retrying; please verify table state if this recurs. Cause: {}",
            partitionValues,
            jobFiles.size(),
            e.toString());
      }
    }

    private Expression scanPruningFilter() {
      // A best-effort prune (shard is always identity-partitioned, so this pushes down cleanly)
      // on top of the exact-path check below; correctness never depends on this filter alone,
      // only on the wantedPaths membership check.
      return Expressions.equal(shardColumnName, shardValue);
    }

    private List<FileScanTask> orderLikeJobFiles(List<FileScanTask> tasks) {
      Map<String, FileScanTask> byPath = new HashMap<>();
      for (FileScanTask t : tasks) {
        byPath.put(t.file().location(), t);
      }
      List<FileScanTask> ordered = new ArrayList<>(jobFiles.size());
      for (FileInfo f : jobFiles) {
        ordered.add(byPath.get(f.path()));
      }
      return ordered;
    }

    private DataFile writeMergedDataFile(List<FileScanTask> tasks, StructLike outputPartition) throws IOException {
      OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PARQUET).build();
      EncryptedOutputFile encryptedOutputFile = fileFactory.newOutputFile(table.spec(), outputPartition);
      String outputLocation = encryptedOutputFile.encryptingOutputFile().location();

      try {
        // GenericFileWriterFactory (rather than calling Parquet.writeData(...) directly) is what
        // keeps this class from needing org.apache.parquet.schema.MessageType - or any other
        // Parquet-specific class - on its compile classpath at all: the factory looks up the
        // registered Parquet FormatModel (from iceberg-parquet, needed only at runtime) via
        // FormatModelRegistry internally. See the class Javadoc's dependency note.
        //
        // NOTE: GenericFileWriterFactory.builderFor(Table) is not public as shipped in 1.11.0
        // (only the Builder class and its constructor are) - construct the Builder directly
        // rather than going through that static factory method.
        FileWriterFactory<Record> writerFactory =
            new GenericFileWriterFactory.Builder(table).dataSchema(table.schema()).dataFileFormat(FileFormat.PARQUET).build();
        DataWriter<Record> writer = writerFactory.newDataWriter(encryptedOutputFile, table.spec(), outputPartition);
        try {
          for (FileScanTask t : tasks) {
            GenericDeleteFilter deleteFilter =
                new GenericDeleteFilter(table.io(), t, table.schema(), table.schema());
            try (CloseableIterable<Record> filtered = deleteFilter.filter(openRows(t, deleteFilter.requiredSchema()))) {
              for (Record r : filtered) {
                writer.write(r);
              }
            }
          }
        } finally {
          writer.close();
        }
        return writer.toDataFile();
      } catch (IOException | RuntimeException e) {
        safeDelete(outputLocation);
        throw e;
      }
    }

    private CloseableIterable<Record> openRows(FileScanTask task, Schema readSchema) {
      DataFile file = task.file();
      FileIO io = table.io();
      InputFile inputFile = io.newInputFile(file.location());
      if (file.format() != FileFormat.PARQUET) {
        throw new UnsupportedOperationException(
            "IcebergPartitionCompactor only reads Parquet data files (kept dependencies minimal); "
                + "got "
                + file.format()
                + " for "
                + file.location());
      }
      // Same rationale as writeMergedDataFile: go through the format registry rather than
      // Parquet.read(...) directly, so this class never needs to compile against Parquet's own
      // classes.
      return FormatModelRegistry.readBuilder(FileFormat.PARQUET, Record.class, inputFile)
          .project(readSchema)
          .split(task.start(), task.length())
          .build();
    }

    private void safeDelete(String location) {
      try {
        table.io().deleteFile(location);
      } catch (RuntimeException e) {
        LOG.warn("Failed to clean up file {} (leaving it in place): {}", location, e.toString());
      }
    }
  }

  // ---------------------------------------------------------------------------------------
  // Schema / partition-spec resolution helpers.
  // ---------------------------------------------------------------------------------------

  private static int positionOf(Types.StructType type, int fieldId) {
    List<Types.NestedField> fields = type.fields();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).fieldId() == fieldId) {
        return i;
      }
    }
    throw new IllegalStateException("Field id " + fieldId + " not present in partition type " + type);
  }

  private static Types.NestedField requireField(Schema schema, String name) {
    Types.NestedField field = schema.findField(name);
    if (field == null) {
      throw new IllegalArgumentException("Column '" + name + "' was not found in the table schema");
    }
    return field;
  }

  private static void requireTimestampType(Types.NestedField field) {
    if (!field.type().typeId().name().contains("TIMESTAMP")) {
      throw new IllegalArgumentException(
          "Column '" + field.name() + "' must be a timestamp type for file ordering, was " + field.type());
    }
  }

  private static void requireIntegerType(Types.NestedField field) {
    if (field.type().typeId() != Type.TypeID.INTEGER) {
      throw new IllegalArgumentException(
          "Shard column '" + field.name() + "' must be an integer type, was " + field.type());
    }
  }

  private static PartitionField findDatePartitionField(PartitionSpec spec, Types.NestedField sourceField) {
    for (PartitionField pf : spec.fields()) {
      if (pf.sourceId() != sourceField.fieldId()) {
        continue;
      }
      Transform<?, ?> transform = pf.transform();
      if (transform.isIdentity() || "day".equals(transform.toString())) {
        return pf;
      }
    }
    throw new IllegalArgumentException(
        "No identity or day-transform partition field found for date source column '"
            + sourceField.name()
            + "' in spec "
            + spec);
  }

  private static PartitionField findIdentityPartitionField(PartitionSpec spec, Types.NestedField sourceField) {
    for (PartitionField pf : spec.fields()) {
      if (pf.sourceId() == sourceField.fieldId() && pf.transform().isIdentity()) {
        return pf;
      }
    }
    throw new IllegalArgumentException(
        "No identity partition field found for column '" + sourceField.name() + "' in spec " + spec);
  }

  private static String requireNonBlank(String value, String paramName) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(paramName + " must not be null or blank");
    }
    return value;
  }
}