#!/usr/bin/env python3
"""
bq-workflow.py — Generate polyconf job args and store in BigQuery.

Usage:
  ./bq-workflow.py <type|all> <env>      — generate args for job type(s)

Samples:
  ./bq-workflow.py delta2bq eng          — generate delta2bq args for eng
  ./bq-workflow.py all eng               — generate all job types for eng
"""

import sys, json, subprocess, pathlib

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
WORKFLOW_YAML = SCRIPT_DIR / "bq-workflow.yaml"

TABLE = "configs.workflow_args"

PROJECT_MAP = {
    "eng":  "polyconf-eng",
    "ver":  "polyconf-ver",
    "exe":  "polyconf-exe",
    "sand": "polyconf-sand",
}


def run_shell(cmd, error_desc):
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        die(f"{error_desc}\nCommand: {cmd}\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}")
    return proc


def run_sql(sql, error_desc, project):
    project_flag = f"--project_id={project} " if project else ""
    cmd = f"bq query {project_flag}--nouse_legacy_sql '{sql}'"
    return run_shell(cmd, error_desc)


def load_yaml(path):
    import yaml
    with open(path) as f:
        return yaml.safe_load(f)


def build_env_vars(data: dict, env: str) -> dict:
    vars_node = data.get("vars", {}) or {}
    merged = {**(vars_node.get("all", {}) or {}), **(vars_node.get(env, {}) or {})}
    merged["Environment"] = env
    merged["YAMLPATH"] = str(SCRIPT_DIR)
    return merged


def expand_command(raw_cmd: str, env: str, data: dict) -> str:
    mapping = build_env_vars(data, env)
    from string import Template
    cmd = Template(raw_cmd).safe_substitute(mapping)
    packages = (
        "--packages io.delta:delta-spark_2.13:3.2.0,"
        "org.apache.spark:spark-sql_2.13:3.5.2"
    )
    cmd = cmd.replace(
        "run.local Args",
        f"spark-submit {packages} --class org.polyconf.argfmt.CliArgGenerator "
        f"target/scala-2.13/polyconf-spark-assembly-0.1.0-SNAPSHOT.jar"
    )
    return cmd


def save_to_bq(job_type: str, env: str, node: dict, data: dict):
    description = node.get("description", "")
    command = node.get("command")
    if not command:
        print(f"Skip {job_type}: no command field", file=sys.stderr)
        return

    expanded = expand_command(command, env, data)
    print(f"Generating args for {job_type} ({env})...")
    proc = run_shell(expanded, f"Generation failed for {job_type}")

    lines = [l for l in (proc.stderr + proc.stdout).splitlines() if l.strip()]
    args_value = lines[-1] if lines else ""
    if not args_value:
        die(f"Empty args generated for {job_type}")

    print(f"  args: {args_value[:200]}...")

    payload = json.dumps({"description": description, "type": job_type, "args": args_value})
    with open("/tmp/bq_workflow_payload.json", "w") as f:
        f.write(payload + "\n")

    project = PROJECT_MAP.get(env)
    project_prefix = f"--project_id={project} " if project else ""

    run_sql(f"DELETE FROM `{TABLE}` WHERE type = '{job_type}'",
            f"Delete old record for {job_type} failed", project)
    run_shell(
        f"bq load {project_prefix}--source_format=NEWLINE_DELIMITED_JSON "
        f"{TABLE} /tmp/bq_workflow_payload.json",
        f"BQ load failed for {job_type}"
    )
    print(f"  saved to {TABLE}")


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    if len(sys.argv) < 3:
        die(__doc__)

    job_type = sys.argv[1]
    env = sys.argv[2]

    data = load_yaml(WORKFLOW_YAML)

    targets = [
        k for k, v in data.items()
        if not k.endswith("-job")
        and isinstance(v, dict)
        and "command" in v
    ] if job_type == "all" else [job_type]

    for t in targets:
        if t not in data:
            print(f"Unknown job type: {t}", file=sys.stderr)
            continue
        save_to_bq(t, env, data[t], data)


if __name__ == "__main__":
    main()
