# polyconf-spark

**Spark data pipeline runner — DataFrame-native transformers, writers, and job orchestration.**

## Overview

polyconf-spark is the Spark integration module for the polyconf framework. It provides:

- **`DFData`** — A `StreamDataBase` implementation wrapping a Spark `DataFrame`, with a `countPass` mechanism using `LongAccumulator` for deferred row counting
- **`SparkTransformerJob`** — A `TransformerJob[DFData]` that creates/manages a SparkSession
- **Spark-native components** — `SparkGeneratorImpl`, `SparkBasicTransformer`, `SparkWriterImpl`, `SparkStatsWriter`
- **Adapter layer** — `StreamDataGenerator`, `StreamDataTransformer`, `StreamDataWriter` that wrap core `StreamData`-based components to work with `DFData`
- **Datasources** — `SparkFileDataIO`, `SparkBqIO`, `SparkPubsubIO`, `SparkEsIO` registered via `SparkDataIO` registry
- **Spark infrastructure** — `SparkSessionInit`, `CliSparkInit`, `SparkLogRelay`, `SparkAccStore`, `SparkCtxStorage`
- **CLI integration** — YAML config via `CliArgGenerator`, job execution via `CliRun`

### Relationship to polyconf-core

```
polyconf-core (no heavy dependencies)
├── StreamDataBase            — base trait: countPass / persist / unpersist
├── StreamData                — in-memory Map-based data container
├── TransformerJob[T]         — generic pipeline engine
├── StreamGenerator[T]        — data source trait
├── StreamTransformer[T]      — transform trait
├── StreamWriter[T]           — sink trait
├── PolyConf                  — polymorphic class dispatch
├── PolyConfProvider (SPI)    — class registration
├── CliArgGenerator           — YAML → CLI args
└── CliRun                    — job executor

polyconf-spark (depends on polyconf-core + Spark)
├── DFData                    — StreamDataBase impl wrapping DataFrame
├── SparkTransformerJob       — TransformerJob[DFData] with Spark lifecycle
├── SparkBasicTransformer     — DataFrame filter/select/repartition/cache
├── SparkGeneratorImpl        — DataFrame file reader
├── SparkWriterImpl            — DataFrame file writer
├── SparkStatsWriter           — DataFrame stats collector
├── StreamDataGenerator        — adapter wrapping StreamGenerator[StreamData] → DFData
├── StreamDataTransformer       — adapter wrapping StreamTransformer[StreamData] → DFData
├── StreamDataWriter            — adapter wrapping StreamWriter[StreamData] → DFData
├── StreamDataAdapter          — StreamData ↔ DataFrame converter
├── SparkFileDataIO / BqIO / PubsubIO / EsIO  — datasources
├── SparkSessionInit            — session creation + configuration
├── SparkLogRelay               — distributed log capture
├── SparkPolyConfProvider       — SPI provider registering all Spark classes
└── SparkCtxStorage / SparkAccStore  — executor-side storage
```

## Design philosophy

polyconf-spark follows the same three design rules as polyconf-core:

### 1. All external parameters as `val`, initialized from JSON/YAML config

Every generator, transformer, writer, and datasource declares its configuration as
`val` fields with sensible defaults. Jackson fills them directly from the config JSON:

```scala
// YAML config:
//   CN: SparkBasicTransformer
//   sqlFilter: "age > 25"
//   selectColumns: [name, age]
//   repartition: 4

// Class — vals populated by Jackson reflection from YAML:
final case class SparkBasicTransformer(
    sqlFilter: String = "",
    selectColumns: Seq[String] = Seq.empty,
    repartition: Int = -1,
    cacheEnabled: Boolean = false
) extends StreamTransformer[DFData]
```

Parents supply defaults; children inherit them without redeclaration:
```scala
trait FileSource { self: PolyConf =>
  val path: String = ""
  val format: String = "text"
  val options: Map[String, String] = Map.empty
}

class SparkWriterImpl extends StreamWriter[DFData] with FileSource
// YAML: {"CN":"SparkWriterImpl", "path":"/out", "format":"parquet", "options":{"header":"true"}}
```

Spark datasources follow the same pattern:
```scala
class SparkFileDataIO extends PolyConf {
  val format: String = "parquet"
  val options: Map[String, String] = Map.empty
}
```

### 2. Eliminate raw `try/catch/finally` — use `Try`, `withResource`, `withData`

No bare `try/catch/finally` blocks. Use `scala.util.Try` / `scala.util.Using` / `Using.resource` instead. Never silently swallow exceptions — if handling an error, log at `error` level:

```scala
// output.write returns Try[Unit]
val result: Try[Unit] = output.write(df).recover {
  case e => logger.warn(s"write failed for $path: ${e.getMessage}")
}
```

Spark resources (sessions, connectors) are managed through `withResource`:
```scala
PolyUtil.withResource(buildReader(file, conf)) { reader =>
  // reader auto-closed on completion or exception
}
```

The full adapter layer uses `Try` throughout — a failed conversion surfaces as
`Failure`, not a thrown exception.

### 3. Minimize global state — prefer instance-level state

Global mutable state is confined to registries that are populated once at startup:

| Global | Purpose |
|--------|---------|
| `SparkDataIO.registry` | Datasource format → class lookup |
| `SparkPolyConfProvider` | SPI registration of all Spark classes |
| `SparkLogRelay` | LongAccumulator for distributed log capture |
| `SparkCtxStorage` / `SparkAccStore` | Executor-side thread-local state |

Everything else lives inside job instances:
- SparkSession is created and stopped per `SparkTransformerJob` run
- Performance counters (`perf` in `TransformerJob`) are instance-level
- `DFData` instances are transient — created per envelope, not cached globally

This means multiple `SparkTransformerJob` instances can coexist in the same JVM
without clashing on session config or metric stores.

## Architecture

### Pipeline data flow

```
Generator (DFData)
  ↓
Transformers (foldLeft)
  ├── Each transformer output → countPass wrapper (countFn saved in stageCountFnBuilders)
  ├── Multi-output transformers produce multiple envelopes
  ↓
Foreach envelope:
  ├── dp.countPass → (counted, countFn)           ← deferred counting
  ├── if (writers.size > 1) counted.persist         ← cache before multi-write
  ├── For each writer:
  │     write(countedData)                         ← first write fills cache
  │     subsequent writes read cache → no re-count
  ├── counted.unpersist
  ├── val rowCount = countFn()                     ← accumulator now populated
  └── writtenCount += rowCount
↓
After all envelopes:
  ├── stageCountFnBuilders.map(_.result().map(_()).sum)  ← intermediate counts
  ├── genFn()                                        ← generator count
  ├── perf.add("genRows", ...)
  ├── perf.add("tr1", ...), perf.add("tr2", ...)     ← per-transformer
  └── perf.add("written", ...)
```

### DFData (`src/main/scala/org/polyconf/spark/stream/DFData.scala`)

```scala
case class DFData(df: DataFrame, options: Map[String, Any] = Map.empty) extends StreamDataBase
```

**`countPass`** — creates a `LongAccumulator` and wraps the DataFrame's RDD:
```scala
val acc = spark.sparkContext.longAccumulator("rowCount-" + UUID.randomUUID())
val countedDf = spark.createDataFrame(
  df.rdd.map { row => acc.add(1L); row },
  df.schema
)
(DFData(countedDf, options), () => acc.value)
```

- The accumulator is populated when the RDD is consumed (by a Spark action)
- The `() => Long` closure captures the accumulator reference
- Note: each `countPass` call creates a new accumulator that lives for the Spark
  application lifetime (pending cleanup improvement S8)

**`persist`** — calls `df.cache()` and returns `this`.  
**`unpersist`** — calls `df.unpersist()` and returns `this`.

### StreamDataAdapter (`src/main/scala/org/polyconf/spark/stream/StreamDataAdapter.scala`)

Converts between `StreamData` (core's `Seq[Map[String, Any]]`) and `DataFrame`:

- **`toDataFrame(spark, packet: StreamData): DataFrame`** — Infers schema from the
  first row's types, maps each row to a Spark `Row`, creates the DataFrame.
  Empty input → empty DataFrame with `_empty` BooleanType column.

- **`fromDataFrame(df, truncationLength: Int = 1000000): StreamData`** — Collects
  rows using a single `limit(n+1).collect()` action (not two separate Spark actions).
  Warns if the result is truncated. Default max: 1,000,000 rows.

Type mapping: `String→StringType, Int→IntegerType, Long→LongType, Double→DoubleType,
Float→FloatType, Boolean→BooleanType, BigInt→DecimalType(38,0), BigDecimal→DecimalType,
Seq→ArrayType(StringType), Map→MapType(String,String), Date→DateType, Timestamp→TimestampType`.

### StreamData adapter layer

Three adapter classes wrap core `StreamData`-based components to work in a `DFData` pipeline:

| Adapter | Wraps | Conversion |
|---------|-------|------------|
| `StreamDataGenerator` | `StreamGenerator[StreamData]` | Generator produces `StreamData` → `StreamDataAdapter.toDataFrame` → `DFData` |
| `StreamDataTransformer` | `StreamTransformer[StreamData]` | Input `DFData → StreamData` via `fromDataFrame`, delegate transform, output `StreamData → DFData` via `toDataFrame`. Configurable `truncationLength` |
| `StreamDataWriter` | `StreamWriter[StreamData]` | Input `DFData → StreamData` via `fromDataFrame`, delegate write. Configurable `truncationLength` |

These allow reusing core polyconf generators/transformers/writers in Spark pipelines
without modification.

### SparkTransformerJob (`src/main/scala/org/polyconf/spark/stream/SparkTransformerJob.scala`)

```scala
final case class SparkTransformerJob(
    override val sparkConfig: Map[String, String] = Map.empty,
    override val appName: String = "polyconf-spark-job",
    override val master: String = "local[*]"
) extends TransformerJob[DFData] with CliSparkInit
```

- **`run()`:** calls `createSpark()` → `super.run()` → `stopSpark(spark)`.
  Catches exceptions and returns `"FAIL: ${e.getMessage}"` instead of throwing.
- Returns the actual `super.run()` result (not a hardcoded string).
- `CliSparkInit` mixin provides the Spark session lifecycle with configurable
  `sparkConfig`, `appName`, `master`.

### SparkGeneratorImpl (`src/main/scala/org/polyconf/spark/stream/SparkGeneratorImpl.scala`)

Reads files or directories as DataFrames. Uses `SparkDataIO` registry for supported
formats, falls back to generic `spark.read.format(...)`.

- Handles directories by iterating files via `Option(file.listFiles())`
- Supports partition control (`partition` field)

### SparkBasicTransformer (`src/main/scala/org/polyconf/spark/stream/SparkTransformerImpl.scala`)

```scala
final case class SparkBasicTransformer(
    sqlFilter: String = "",
    selectColumns: Seq[String] = Seq.empty,
    repartition: Int = -1,
    cacheEnabled: Boolean = false
)
```

Applies DataFrame operations in order: `select` → `filter(sqlFilter)` → `repartition` → `cache`.
Each field is optional (disabled by its default value).

### SparkWriterImpl (`src/main/scala/org/polyconf/spark/stream/SparkWriterImpl.scala`)

Writes DataFrames using `SparkDataIO` registry (format lookup) or generic Spark API.

- `repartitionIfNeeded` adjusts partitions only when `partition > 0`
- Falls back to `df.write.format(format).mode(mode).options(options).save(path)`
  when no registered datasource matches

### SparkStatsWriter (`src/main/scala/org/polyconf/spark/stream/SparkWriterImpl.scala`)

Statistics writer for development/debugging:

```scala
class SparkStatsWriter extends StreamWriter[DFData] {
  val showDebugRowsNumber: Int = 10
  val showRows: Boolean = false
  val showWorkerRows: Int = 0
}
```

- `write()`: caches DF → shows count, partitions, optional sample rows on driver
  (`showRows`), optional distributed row logging via `SparkLogRelay`
  (`showWorkerRows`). Unpersists in `try/finally`.

## Datasources (`org.polyconf.spark.datasource`)

### SparkDataIO registry

`SparkDataIO` is a trait + object providing a `TrieMap`-based registry:

```scala
trait SparkDataIO {
  def name: String
  def reader(df: DataFrame, path: String, options: Map[String, String]): DataFrame
  def writer(df: DataFrame, path: String, mode: String, options: Map[String, String]): Unit
  def removeStorage(path: String): Unit = {}
  def help: String
}
```

Registered datasources are dispatched by format name in `SparkGeneratorImpl`,
`SparkWriterImpl`, and `SparkSessionInit.create()`.

### SparkFileDataIO

| Format | Reader | Writer | Notes |
|--------|--------|--------|-------|
| `avro` | `format("avro")` | `format("avro")` | |
| `parquet` | `format("parquet")` | `format("parquet")` | |
| `text` | `format("text")` | Converts all columns to JSON string | Only JSON column is written |
| `json` | `format("json")` | `format("json")` | |
| `csv` | `inferSchema=true, header=true, quote=", escape=\` | `header=true, quote=", escape=\` | |
| `delta` | `format("delta")` | `format("delta")` | `removeStorage` is no-op for Delta |

### SparkBqIO

BigQuery integration via `com.google.cloud.spark.bigquery`:
- **Reader:** `query` option (SQL) or `table` option (table reference)
- **Writer:** `writeMethod` (`direct`/`indirect`), `temporaryGcsBucket`, `parallelism`
- **removeStorage:** `DROP TABLE IF EXISTS` via SQL (redundant call removed — S3 fix)

### SparkPubsubIO

Google Cloud Pub/Sub via `GrpcSubscriberStub`/`Publisher`:
- **Reader:** Pulls messages with optional `maxMessages` limit, creates DataFrame
  with `data` (String) and `ackId` (String) columns
- **Writer:** `foreachPartition` publishes each row as `PubsubMessage`
- **Acknowledge:** `foreachPartition` sends `AcknowledgeRequest` via
  `SubscriptionAdminClient`
- **removeStorage:** Deletes subscription
- **Path format:** `projectId:topicId:subscriptionId`, default project from
  `GOOGLE_CLOUD_PROJECT` env
- **Pending improvements:** S9 (swallowed exceptions in acknowledge),
  S10 (no publisher `awaitTermination`), S11 (ignores `mode`)

### SparkEsIO

Elasticsearch via `org.elasticsearch:elasticsearch-spark-30`:
- Uses `format("es")` with options passthrough
- **removeStorage:** HTTP DELETE to index URL
- **Auth:** ApiKey (`es.net.http.auth.pass` with `es.net.http.auth.user=ApiKey`)
  or Basic auth
- **Pending:** duplicated `removeStorage` before write, `"UTF-8"` literal instead
  of `StandardCharsets.UTF_8`

## Spark infrastructure

### SparkSessionInit (`src/main/scala/org/polyconf/spark/core/SparkSessionInit.scala`)

Creates and configures the Spark session:
1. Reads `spark.polyconf.logRules` from config (for executor logging)
2. Calls `SparkLogRelay.init(sc)` to register the logging accumulator
3. Registers all `SparkDataIO` datasources: `SparkFileDataIO`, `SparkBqIO`,
   `SparkPubsubIO`, `SparkEsIO`
4. Re-applies `log4j2.xml` after Spark's default log override
5. Sets `sparkContext.setLogLevel("WARN")` to suppress noisy internal logs

### CliSparkInit (`src/main/scala/org/polyconf/spark/core/CliSparkInit.scala`)

Mixin trait for components that need a Spark session:
```scala
trait CliSparkInit extends PolyConf {
  val sparkConfig: Map[String, String] = Map.empty
  val appName: String = "polyconf-spark-job"
  val master: String = "local[*]"
  def createSpark(): SparkSession = SparkSessionInit.create(...)
  def stopSpark(spark: SparkSession): Unit = SparkSessionInit.stop(spark)
}
```

### SparkLogRelay (`src/main/scala/org/polyconf/spark/core/SparkLogRelay.scala`)

Distributed logging: captures log events on executors using `CaptureAppender` and
transfers them to the driver via `CollectionAccumulator[String]`.

- `init(sc)`: registers the accumulator, propagates log rules to executors
- `getLogger(name)`: creates a CaptureAppender for the given logger
- `flushWorkerLogs(acc)`: drains all CaptureAppenders into the accumulator
- `printMasterLogs()`: flushes + prints captured logs on the driver
- Resolves log levels from `spark.polyconf.logRules` using `PolyLog.getLevel`
- Uses log4j 2.x `Configuration` API to add appenders programmatically

### SparkCtxStorage (`src/main/scala/org/polyconf/spark/core/SparkCtxStorage.scala`)

Key-value store using `SparkContext.setLocalProperty`/`getLocalProperty`:
- Keys prefixed with `"polyconf.spark."` to avoid collisions
- Values are Java-serialized via `PolySerde.javaSerialize`/`javaDeserialize`
- Supports String, Int, Map, and any `Serializable` type

### SparkAccStore (`src/main/scala/org/polyconf/spark/core/SparkAccStore.scala`)

Thread-safe (`TrieMap`) registry of named Spark `LongAccumulator`s:
- `register(name, sc)`: creates or retrieves a named accumulator
- `getValue(name)`: returns current value (or 0 if missing)
- `clear()`: empties the registry

### SparkPolyConfProvider (`src/main/scala/org/polyconf/spark/core/SparkPolyConfProvider.scala`)

SPI provider that registers all Spark-specific classes:
```scala
class SparkPolyConfProvider extends PolyConfProvider {
  private val (concrete, bases) = PolyConfProvider.registerAllChildForBases(
    classOf[TransformerJob[_]], classOf[StreamGenerator[_]],
    classOf[StreamTransformer[_]], classOf[StreamWriter[_]],
    classOf[ParamsGeneratorBase]
  )
  ...
}
```

Registered via `META-INF/services/org.polyconf.core.PolyConfProvider`.

## Log levels

### Two-phase log application

Log levels flow through two phases:

1. **Phase 1 (CLI arg generation):** `CliArgGenerator` parses `--logRules`/`-l` and
   applies via `PolyLog.setLogRules()`
2. **Phase 2 (job execution):** `CliRun` parses the same flag again

To set both phases from YAML:
```yaml
Formatter:
  CN: RunParamsGenerator
  shellPrefix: ./run.sh run.spark
  additionalArgs:
    - -l
    - DEBUG
```

### Log format

Console output uses `log4j2.xml` pattern with ANSI colors and file:line:
```
%d{HH:mm:ss.SSS} [%t] %highlight{%-5p} (%F:%L) %highlight{%-20M} - %msg%n
```

Example:
```
12:34:56.789 [pool-1-thread-1] INFO  (SparkTransformerJob.scala:32) run - TransformerJob started
```

### Executor-side logging

Log levels propagate to executors via `spark.polyconf.logRules` config.
`SparkLogRelay.resolveLogRules()` reads it from `SparkEnv` on each executor and
configures log4j. `CaptureAppender` captures events into a Spark accumulator for
driver-side retrieval.

## Testing

### Test framework

ScalaTest 3.2.19 with `AnyFlatSpec` + `Matchers` + `BeforeAndAfterAll`.
Tests fork JVM, use `Flat` classloader layering.

### Test helpers

**`TestSparkMaster`** — resolves Spark master URL from sys prop → env → `local[1]`.
Provides `config` with `spark.executor.extraClassPath` for standalone mode.

**`TestDataComponents`** — inline test components:
- `TestDataGenerator(numRows, numPartitions)` — DataFrame with `id` (Int), `group` (String)
- `TestDataTransformer(filterExpr)` — SQL filter
- `TestDataWriter(targetPartitions)` — writes to `noop` format

### Test suites (61 tests)

| Suite | Tests | What it covers |
|-------|-------|----------------|
| `TransformerJobPerfSpec` | 9 | Performance tracking: genRows/written counters, single/double transformers, two writers, zero rows, filter-to-zero, multi-file, entries keys, stage consistency |
| `SparkTransformerJobSpec` | 9 | JSON deserialization + polymorphic construction of `SparkTransformerJob` with generators, transformers, writers, config fields |
| `SparkTransformerSpec` | 7 | `SparkBasicTransformer` filtering, empty DF, chaining select+filter+repartition, no-op, caching |
| `CountPassSpec` | 5 | `DFData.countPass` correctness: single write, multi-partition, chained gen+transformer, persist+multi-writer, end-to-end with `TransformerJob` |
| `SparkDataFrameSpec` | 11 | DFData construction, countPass after action, empty DF, schema round-trip, SparkBasicTransformer ops, StreamDataAdapter to/from DataFrame, empty packet |
| `SparkSessionInitSpec` | 5 | Session creation, datasource registration, JSON/Parquet read/write, WriteMode values |
| `SparkDistributedSpec` | 6 | `SparkAccStore` merged accumulators, `SparkLogRelay` worker log capture, multi-logger, concurrent flush, combined pipeline |
| `SparkLogRelaySpec` | 6 | Logger creation, capture+flush, multi-message, master visibility, clear, level filtering from config |
| `SparkCtxStorageSpec` | 6 | Put/get string/int/map, missing key, remove, key overlap |

### sbt commands

| Command | Description |
|---------|-------------|
| `sbt compile` | Compile main sources |
| `sbt test` | Run all 61 tests |
| `sbt "testOnly org.polyconf.spark.stream.TransformerJobPerfSpec"` | Run a single suite |
| `sbt "testOnly org.polyconf.spark.stream.TransformerJobPerfSpec -- -z \"generator-only\""` | Run specific test by name substring |
| `sbt "testOnly org.polyconf.spark.stream.CountPassSpec"` | Run the countPass spec |
| `sbt assembly` | Build fat JAR |
| `sbt clean compile` | Clean and recompile |

### Dependency note

polyconf-spark depends on polyconf via published artifact:
```scala
"io.github.pyal" %% "polyconf" % "0.1.0-SNAPSHOT"
```

After changing polyconf source, you must run `sbt publishLocal` in the polyconf
directory before polyconf-spark tests will pick up the changes.

## run.sh

```
./run.sh run.spark ...     # spark-submit --class CliRun (assembly JAR)
./run.sh run.args ...      # CliArgGenerator (assembly JAR)
./run.sh run.dev.args ...  # CliArgGenerator via sbt (no JAR)
./run.sh run.dev.local ... # CliRun via sbt (no JAR)
```

## test.sh

Runs 5 integration tests from `scripts/sparkJob.yaml`:
1. YAML → spark-submit (assembly)
2. YAML generation only
3. Direct spark-submit with JSON
4. Partition test (generator partition=10 → writer partition=2)
5. StatsWriter test (showWorkerRows=6, partition=3)

## Code review status

Issues from `REVIEW_ISSUES.md` categorized by severity:

**CRITICAL (all fixed):**
| ID | Description | Fix |
|----|-------------|-----|
| S1 | `SparkTransformerJob.run()` ignored `super.run()` return value | Use `super.run()` result |
| S2 | `StreamDataAdapter.fromDataFrame` collected entire DF to driver | Added `truncationLength` param |
| S3 | `SparkBqIO` redundant `DROP TABLE` created data-loss window | Removed redundant block |
| S4 | `SparkPubsubIO.readMessages` unbounded pull | Skipped (deliberate default) |

**HIGH (all fixed):**
| ID | Description | Fix |
|----|-------------|-----|
| S5 | `fromDataFrame` double Spark action | Single `limit(n+1).collect()` |
| S6 | `SparkStatsWriter` missing `unpersist()` | Wrapped in `try/finally` |

**MEDIUM (all pending):**
| ID | Description |
|----|-------------|
| S7 | `SparkWriterImpl` ignores `frameData.options` |
| S8 | `DFData.countPass` leaks accumulators |
| S9 | `SparkPubsubIO.acknowledgeMessages` swallows exceptions |
| S10 | `SparkPubsubIO.writeMessages` no `awaitTermination` |
| S11 | `SparkPubsubIO.writer` ignores `mode` |
| S12 | `SparkCtxStorage` unhelpful `ClassCastException` |

**LOW:**
| ID | Description | Status |
|----|-------------|--------|
| S13 | `printMasterLogs()` called twice | Fixed: removed redundant call |
| S14 | `SparkPolyConfProvider` registration repeats | Pending |
| S15 | Datasources re-registered on every `SparkSessionInit.create()` | Pending |
| S16 | `SparkLogRelay.init()` called multiple times leaks accumulator | Pending |
