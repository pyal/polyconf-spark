# bq-integration-sample

Sample demonstrating how polyconf integrates with BigQuery to store workflow parameters.

## Concept

Two workflows cover all execution:

- **General workflow** (single endpoint) — accepts any polyconf command. Covers all admin tasks, manual triggers, ad-hoc runs. No configuration changes needed when jobs are added.
- **Typed workflows** (one per cron job) — read the BigQuery configs table **by type** to load the proper defaults for that environment. Params are defined in `bq-workflow.yaml`, which sets defaults against `polyconf.yaml` and exposes overridable variables. This centralises param definitions so all workflow params can be regenerated after class interface changes.

## Files

| File | Purpose |
|------|---------|
| `polyconf.yaml` | All typed jobs with env-specific defaults (Renamer + Formatter + jobStr format) |
| `bq-workflow.yaml` | Typed workflow param definitions: type → description + CliArgGenerator command |
| `bq-workflow.py` | Generates args for each job type and saves to `configs.workflow_args` in BQ |
| `medium-post.md` | Draft Medium article describing the full approach |

## Flow

```
polyconf.yaml  ──►  bq-workflow.py  ──►  BigQuery configs.workflow_args
                                               │
                          Typed workflows ──────┘  (read by type, execute with defaults)
```

## Using @JsonIgnore in PolyConf classes

Fields that should not appear in JSON output (internal caches, accumulators, computed values) can be excluded with `@JsonIgnore`:

```scala
import com.fasterxml.jackson.annotation.JsonIgnore

class AgeFilterTransformer extends StreamTransformer[DataStream] {
  val minAge: Int = 18       // from JSON config
  @JsonIgnore private val logger = LoggerFactory.getLogger(getClass)  // internal only
}
```

To add a new job:
1. Add entry in `polyconf.yaml`
2. Add workflow entry in `bq-workflow.yaml`
3. Run `./bq-workflow.py <type> <env>` to push args to BQ
