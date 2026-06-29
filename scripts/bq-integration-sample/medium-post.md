# Your Pipelines Are YAML Now: Config-Driven Polymorphism in Scala

*20 pipelines, 30 cron jobs, one YAML file. Every job's business logic in plain sight.*

---

## 1. The Problem

**Every new job the normal way:** new repo → new CLI scaffold → new CI/CD wiring → new workflow deployment. Repeat 40 times. Each one takes days.

You have no shell access to test or prod — only workflows. And you need to manage:

- 20 Spark pipelines varying by input (BQ/GCS/BigTable), filters, and output format
- 10 Spark cron jobs
- 20 Cloud Run admin jobs — copy files, create/drop tables, Pub/Sub, cleanup
- 10 Cloud Run cron jobs

With the traditional approach, the business logic for each job is buried inside Spark or Java code. To understand what a pipeline does, you have to read through layers of Scala classes. Reviewing a change means understanding the entire class hierarchy. Auditing is painful.

There had to be a better way.

## 2. Core Idea: Classes from JSON, No Prior Knowledge

The core technology is simple: create an arbitrary class from JSON **without any prior knowledge of its real class**.

You define a root class that uses a `CN` field:

```json
{"CN": "SparkBasicTransformer", "sqlFilter": "age > 25", "selectColumns": ["name", "age"]}
```

The `CN` value is a class name. At runtime, reflection resolves it to the real class, Jackson deserialises all parameters into its fields, and the object is ready to use. No hardcoded imports, no pattern match, no factory.

This works because every configurable class declares its parameters as `val` fields with sensible defaults:

```scala
final case class SparkBasicTransformer(
    sqlFilter: String = "",
    selectColumns: Seq[String] = Seq.empty,
    repartition: Int = -1,
    cacheEnabled: Boolean = false
) extends StreamTransformer[DFData]
```

Jackson sets these fields directly via reflection — no setters, no builders, no constructor injection.

On top of this, we built a simple CLI that reads an arbitrary runnable class (any class with a single `run()` method) from a JSON config and executes it. The same CLI runs every job:

- **RunnableShell** — execute shell commands
- **TransformerJob** — a generic pipeline: `generator → transformers[] → writers[]`

To add new functionality, you write a new runnable class. The CLI never changes. No parameter parsing, no new entry points.

## 3. The Two Libraries: polyconf and polyconf-spark

All this logic is implemented in two libraries.

**polyconf** is the core framework for local (Cloud Run) jobs without Spark or big data support. Data is held as `Seq[Map[String, Any]]` via `DataStream`. It includes the CLI (`CliRun`), the polymorphic deserialisation (`PolyConf`), the YAML-to-args generator (`CliArgGenerator`), and the generic pipeline engine (`TransformerJob[T]`).

**polyconf-spark** extends polyconf to work with big data under Spark on Dataproc. Data becomes an ordinary Spark `DataFrame` wrapped as `DFData`. `TransformerJob` is defined as a template class parameterised over `T <: StreamDataBase` — in polyconf the data type is `DataStream`, in polyconf-spark it's `DFData`.

All generators, transformers, and writers must support their given data type. An adapter layer (`StreamDataAdapter`) converts between `Map[String, Any]` and `DataFrame`, so local filters and transformers can be reused in the Spark environment without modification.

**Logging across workers.** One hard problem in distributed Spark jobs is seeing worker-node logs. `SparkLogRelay` solves this by capturing log events on executors via `CaptureAppender` and transferring them to the driver through a `CollectionAccumulator[String]`. Worker logs appear alongside driver logs — no more digging through cloud logging UIs.

**Datasources.** polyconf-spark supports all basic GCP datasources out of the box:

| Category | Formats |
|----------|---------|
| File / GCS | avro, csv, text, parquet, json, delta |
| BigQuery | read/write with indirect write method |
| Pub/Sub | publish and acknowledge |
| Elasticsearch | read/write with API key auth |

Each datasource is a pluggable `SparkDataIO` implementation registered in a `TrieMap` registry and dispatched by format name.

**Built-in metrics.** `TransformerJob` measures and reports every stage of the pipeline automatically:

- Generator row count (`genRows`)
- Per-transformer output rows (`tr1`, `tr2`, ...)
- Per-writer consumed rows (`wr1`, `wr2`, ...)
- Total written rows and wall-clock time

For local jobs, metrics use thread-safe `PolyAccumulatorPerf` counters. For Spark jobs, they use `LongAccumulator` instances merged across executors. The same `TransformerJob` code works for both — only the accumulator implementation changes.

## 4. One YAML Defines All Jobs

A single file — `polyconf.yaml` — holds every pipeline and admin task with environment-specific variables. All business logic in one place.

```yaml
bq2bq-sync:
  Renamer:
    CN: JsonVariableInline
    Env:
      all:
        BQ_PROJECT: polyconf-$Env
        BQ_DATASET: events
        SRC_TABLE: raw_events
        DST_TABLE: clean_events
        FILTER: "WHERE lastmodifieddate >= '1970-01-01'"
      sand:
        BQ_PROJECT: polyconf-sand
      prod:
        FILTER: "WHERE lastmodifieddate >= '2024-01-01'"
    defaultEnv: eng

  Formatter:
    CN: RunParamsGenerator
    shellPrefix: ./run.sh run.spark
    additionalArgs:
      - -l
      - "WARN,ERROR:org.apache,DEBUG:org.polyconf"
    jobStr:
      CN: SparkTransformerJob
      appName: bq2bq-sync
      generator:
        CN: SparkGeneratorImpl
        format: bigquery
        path: $BQ_PROJECT:$BQ_DATASET.$SRC_TABLE
        options:
          query: "SELECT * FROM `$BQ_PROJECT.$BQ_DATASET.$SRC_TABLE` $FILTER"
      transformers:
        - CN: SparkBasicTransformer
          sqlFilter: "age > 18"
          selectColumns: [id, name, age, city, created_at]
          repartition: 8
      writers:
        - CN: SparkWriterImpl
          format: bigquery
          mode: Overwrite
          path: $BQ_PROJECT:$BQ_DATASET.$DST_TABLE
```

Every variable (paths, tables, filters, worker counts) is defined per environment in the `Env` section. Running the same config against dev, staging, or prod is a single CLI flag:

```bash
./run.sh run.dev.args --yamlPath polyconf.yaml::bq2bq-sync --renameStr "Env--prod"
```

The `CliArgGenerator` tool turns this YAML into executable commands in multiple output formats:

- `--format shell` — shell-escaped command for local execution
- `--format yaml` — raw YAML output
- Pluggable escape formats for workflow submission, BigQuery, or any target

## 5. Two Workflows Cover All Execution

We defined exactly two workflows:

**General workflow** — a single endpoint that accepts any polyconf command from user input. It covers all admin tasks, manual triggers, and ad-hoc runs. When you add a new job to `polyconf.yaml`, it works immediately through this endpoint. No workflow configuration changes needed.

**Typed workflows** — one per cron job. Each reads a BigQuery configs table **by type** to load the proper defaults for its environment. The parameters are defined in `bq-workflow.yaml`, which sets defaults against `polyconf.yaml` and exposes variables for manual override:

```
run.local Args -y polyconf.yaml -m incremental-update \
  --rename-str Env--$Environment~~SINCE--$DEFAULT_SINCE \
  -f bigquery
```

When a class interface changes, you regenerate all workflow params from this single definition file — no hunting through workflow UIs for hardcoded values.

Minor manual overrides are passed as JSON when triggering a workflow:

```
spark.workflow.run polyconf-bqbased-bq2bq-sync '{
  "rename_FILTER": "WHERE lastmodifieddate >= '\''2025-06-01'\''"
}'
```

## 6. The Add-a-Job Workflow

Adding a new job follows a consistent pattern:

**New ad-hoc job:** write a polyconf config in `polyconf.yaml`. Execute it immediately via the general workflow endpoint. No branch, no PR, no deployment.

**New cron job:** write the polyconf config + a `bq-workflow.yaml` entry setting default params and exposing overridable variables. Workflow deployment only when the cron schedule changes.

If you need new functionality, write a new runnable class. Regular JAR deployment via CI/CD — old configs still work unchanged.

All jobs in one file. Business logic in plain YAML. Clear to review. Clear to audit.

## 7. Same Config in Tests and Production

The same polyconf config runs in unit tests and in production. The only difference is environment settings:

```bash
# Test
./run.sh run.dev.args --yamlPath polyconf.yaml::bq2bq-sync --renameStr "Env--test"

# Production
./run.sh run.args --yamlPath polyconf.yaml::bq2bq-sync --renameStr "Env--prod"
```

You test the exact config you deploy.

`VerifyWriter` takes this further. It's one of the standard writers — in tests, it replaces the production writer and compares pipeline output against a previously saved snapshot (source of truth). The same pipeline config, the same generator, the same transformers — only the writer changes. Reliable regression testing without mocks or golden-file management.

## 8. Auto-Registration, @JsonIgnore & Extensibility

All concrete classes are auto-discovered at startup via SPI and classpath scanning. No manual `register()` calls, no XML config, no annotation processing.

**Ignoring fields.** Sometimes a `PolyConf` class has internal fields that should not appear in JSON output — caches, helpers, or precomputed values. Annotate them with `@JsonIgnore`:

```scala
class AgeFilterTransformer extends StreamTransformer[DataStream] {
  // This is populated from JSON config — appears in serialised output
  val minAge: Int = 18

  // This is internal state — excluded from JSON entirely
  @JsonIgnore private val logger = LoggerFactory.getLogger(getClass)
}
```

`@JsonIgnore` works on private vals, constructor parameters, and def getters alike. Use it whenever a field is runtime-only and should never be part of a config JSON.

To extend the framework:

- **New transformer or writer**: extend the base traits, provide a `PolyConfProvider`
- **New datasource**: implement `SparkDataIO` for any file format, add to the registry
- **New escape format**: extend `EscapeFormatBase` — for workflow submission, BigQuery string escaping, or any custom output

Sample usage is in the repo's `test.sh` — all jobs defined in YAML config.

Both libraries are open source: [github.com/pyal/polyconf](https://github.com/pyal/polyconf) and [github.com/pyal/polyconf-spark](https://github.com/pyal/polyconf-spark).

## Summary

Config-driven polymorphism changed how we build data pipelines:

- **One YAML file** defines every job across all environments
- **Two workflows** cover all execution — general (ad-hoc) and typed (cron)
- **One CLI** generates any output format from the same config
- **Same config** runs in tests and production
- **Adding a job** is writing config, not code

Your pipelines are YAML now. The business logic is in plain sight.
