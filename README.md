# polyconf-spark

Spark integration for polyconf — a framework for building config-driven, polymorphic
data pipelines.

Adds Spark-native components to the polyconf pipeline model: generators that read
from distributed storage, transformers that operate on DataFrames, and writers for
Spark datasources including BigQuery, PubSub, Elasticsearch, and Delta Lake.

## Architecture

### `DFData` — DataFrame wrapper with deferred row counting

```scala
case class DFData(df: DataFrame, options: Map[String, Any] = Map.empty)
```

`countPass` creates a `LongAccumulator` and wraps the RDD with a counting map.
The row count is available as a deferred `() => Long` function — populated when
a Spark action consumes the data.

`persist` / `unpersist` delegate to `df.cache()` / `df.unpersist()`.

### `SparkTransformerJob` — Spark-native pipeline runner

Extends `TransformerJob[DFData]` with automatic `SparkSession` lifecycle.
Configurable via `sparkConfig`, `appName`, `master` — all set from YAML/JSON.

### Generators / Transformers / Writers

- **`SparkGeneratorImpl`** — reads files or directories using `SparkDataIO` or
  generic `spark.read.format()`. Delta, Parquet, Avro, JSON, CSV, Text.
- **`SparkBasicTransformer`** — `select`, `filter`, `repartition`, `cache`
- **`SparkWriterImpl`** — writes via `SparkDataIO` registration, with partition
  control and format fallback
- **`SparkStatsWriter`** — development stats: count, partitions, sample rows on
  driver, distributed row logging via `SparkLogRelay`

### Adapter layer

`StreamDataGenerator`, `StreamDataTransformer`, `StreamDataWriter` wrap core
polyconf `StreamData`-based components to work in a `DFData` pipeline — enabling
reuse of non-Spark generators, transformers, and writers without modification.

## Datasources

All registered via `SparkDataIO` and dispatched by format name:

- **`SparkFileDataIO`** — avro, parquet, json, csv, text, delta
- **`SparkBqIO`** — BigQuery read/write with direct or indirect method
- **`SparkPubsubIO`** — Pub/Sub subscribe, publish, acknowledge
- **`SparkEsIO`** — Elasticsearch read/write with ApiKey or Basic auth

## Distributed logging

`SparkLogRelay` captures log4j events on executors into a `CollectionAccumulator`
and merges them on the driver. Configure log levels per logger via
`spark.polyconf.logRules`.

## `run.sh` — CLI entry point

| Command | Mode | Purpose | How it runs |
|---------|------|---------|-------------|
| `run.spark` | Production | Execute a job via `spark-submit` (requires assembly JAR) | `spark-submit --class ... CliRun assembly.jar` |
| `run.args` | Production | Generate CLI args from YAML — prints the generated command | `java -cp assembly.jar org.polyconf.argfmt.CliArgGenerator` |
| `run.dev.args` | Development | Generate CLI args from YAML (via sbt, no JAR generation) | `sbt runMain org.polyconf.argfmt.CliArgGenerator` |
| `run.dev.local` | Development | Execute a job locally via sbt (local Spark on classpath) | `sbt runMain org.polyconf.cli.run.CliRun` |

**Production** modes require `sbt assembly` first (builds the fat JAR with Spark).
**Development** modes use sbt directly — no JAR build needed, runs Spark locally.

```bash
# Generate a spark-submit command from YAML:
./run.sh run.dev.args --yamlPath scripts/sparkJob.yaml::sparkFilterJob

# Generate + pipe into spark-submit to execute:
./run.sh run.dev.args --yamlPath scripts/sparkJob.yaml::sparkFilterJob --execute

# Run a Spark job directly from a JSON string (via sbt, local mode):
./run.sh run.dev.local -j '{"CN":"SparkTransformerJob","appName":"test","generator":{...}}' -l WARN

# Production: generate spark-submit command from YAML (assembly JAR):
./run.sh run.args --yamlPath scripts/sparkJob.yaml::sparkFilterJob

# Production: spark-submit with JSON directly:
SPARK_MASTER=local[1] ./run.sh run.spark -j '{"CN":"SparkTransformerJob",...}' -l WARN
```

## `test.sh` — end-to-end integration tests

```bash
./test.sh                # run all, quiet mode
./test.sh --verbose      # show full command output
./test.sh --help         # show usage
```

Builds the assembly JAR, then runs 5 tests from `scripts/sparkJob.yaml`:

| Test | What it tests |
|------|---------------|
| `sparkFilterJob (assembly + spark-submit)` | YAML → `run.args --execute` — full pipeline via assembly JAR |
| `sparkFilterJob (YAML generation)` | YAML generation only, no execution |
| `sparkFilterJob (spark-submit)` | Direct JSON → `run.spark` — Spark job via `spark-submit` |
| `Partition test` | `SparkGeneratorImpl partition=10` → `SparkWriterImpl partition=2` |
| `StatsWriter test` | `SparkStatsWriter` with `showWorkerRows=6`, `partition=3` |

Each test:
1. Builds or generates the Spark job config
2. Executes via `run.spark` (spark-submit) or `run.args --execute`
3. Validates output files, exit codes, and specific log messages

## Build

```bash
sbt test          # 55 unit tests
sbt assembly      # fat JAR for spark-submit
sbt publishLocal  # publish to local ivy cache
```

Depends on polyconf via published artifact `"com.github.pyal" %% "polyconf" % "0.1.0-SNAPSHOT"`.
After changing polyconf source, run `sbt publishLocal` in the polyconf repo first.
