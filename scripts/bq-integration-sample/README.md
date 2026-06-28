# bq-integration-sample

Sample demonstrating how polyconf integrates with BigQuery to store workflow parameters.

## Concept

A single general-purpose workflow (e.g., GCP Workflows, Cloud Composer) reads job arguments from a BigQuery `configs.workflow_args` table by job type, then executes the corresponding polyconf job with those arguments. This decouples job definitions from workflow deployment:

- **Job definitions** live in `polyconf.yaml` — the single source of truth
- **Workflow commands** live in `bq-workflow.yaml` — map job types to CliArgGenerator invocations
- **Config storage** lives in BigQuery — the general workflow reads from `configs.workflow_args`

## Files

| File | Purpose |
|------|---------|
| `polyconf.yaml` | All typed jobs with env-specific defaults (Renamer + Formatter + jobStr format) |
| `bq-workflow.yaml` | Workflow definitions: type → description + CliArgGenerator command |
| `bq-workflow.py` | Generates args for each job type and saves to `configs.workflow_args` in BQ |

## Flow

```
polyconf.yaml  ──►  bq-workflow.py  ──►  BigQuery configs.workflow_args
                                               │
                          General workflow ─────┘  (reads by type, executes args)
```

To add a new job:
1. Add entry in `polyconf.yaml` — no repos, no deployments
2. Add workflow entry in `bq-workflow.yaml`
3. Run `./bq-workflow.py <type> <env>` to push args to BQ
