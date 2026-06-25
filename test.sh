#!/bin/bash

YAML="scripts/sparkJob.yaml"
verbose=false

usage() {
  echo "Usage: $0 [--verbose|--help]"
  echo "  --verbose  Show full generated command output"
  echo "  --help     Show this help"
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --verbose) verbose=true; shift ;;
    --help)    usage 0 ;;
    *)         usage 1 ;;
  esac
done

ROOT=$(cd "$(dirname "$0")" && pwd)
cd "$ROOT"

echo "Building JAR..."
sbt assembly 2>&1 | tail -5
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: sbt assembly failed"
  exit 1
fi
echo ""

failures=0
total=0

run_exec_test() {
  local desc="$1"
  local cmd="$2"
  local setup="$3"

  total=$((total + 1))
  echo "========================================================================"
  echo "  $desc"
  echo "------------------------------------------------------------------------"
  echo "  Command: $cmd"

  [ -n "$setup" ] && eval "$setup"

  if [ "$verbose" = true ]; then
    echo "  Output:"
    eval "$cmd" 2>&1 | sed 's/^/    /'
    rc=${PIPESTATUS[0]}
  else
    eval "$cmd" > /dev/null 2>&1
    rc=$?
  fi

  if [ $rc -eq 0 ]; then
    echo "  Result: OK"
  else
    echo "  Result: NOT OK (exit code $rc)"
    failures=$((failures + 1))
  fi
}

run_json_test() {
  local desc="$1"
  local json="$2"

  total=$((total + 1))
  echo "========================================================================"
  echo "  $desc"
  echo "------------------------------------------------------------------------"
  echo "  Running: SPARK_MASTER=local[1] ./run.sh run.spark -j '$json'"
  output=$(SPARK_MASTER=local[1] ./run.sh run.spark -j "$json" 2>&1)
  rc=$?
  echo "$output" | sed 's/^/  /'

  # Run check function if defined for this test
  if declare -f "check_${total}" > /dev/null; then
    "check_${total}" "$output" || rc=1
  fi

  if [ $rc -eq 0 ]; then
    echo "  Result: OK"
  else
    echo "  Result: NOT OK (exit code $rc)"
    failures=$((failures + 1))
  fi
}

# ============================================================================
# Test 1: sparkFilterJob1 — YAML→run.spark via assembly (no sbt)
# ============================================================================
run_exec_test "sparkFilterJob (assembly + spark-submit)" \
  "./run.sh run.args --yamlPath $YAML::sparkFilterJob --execute" \
  "rm -rf /tmp/spark-output && mkdir -p /tmp/spark-output"

# ============================================================================
# Test 2: sparkFilterJob — YAML generation only (no execute)
# ============================================================================
run_exec_test "sparkFilterJob (YAML generation)" \
  "./run.sh run.args --yamlPath $YAML::sparkFilterJob"

# ============================================================================
# Test 3: sparkFilterJob (spark-submit)
# ============================================================================
run_exec_test "sparkFilterJob (spark-submit)" \
  "SPARK_MASTER=local[1] ./run.sh run.spark -j '{\"CN\":\"SparkTransformerJob\",\"appName\":\"test-spark\",\"generator\":{\"CN\":\"StreamDataGenerator\",\"delegate\":{\"CN\":\"SimpleDataGenerator\",\"path\":\"scripts/input.csv\",\"format\":\"csv\"}},\"transformers\":[{\"CN\":\"SparkBasicTransformer\",\"sqlFilter\":\"age >= 18\",\"selectColumns\":[\"name\",\"age\",\"city\"]}],\"writers\":[{\"CN\":\"StreamDataWriter\",\"delegate\":{\"CN\":\"SimpleDataWriter\",\"path\":\"/tmp/spark-submit-output.json\",\"format\":\"json\"}}]}'" \
  "rm -rf /tmp/spark-submit-output.json"

# ============================================================================
# Test 4: Partition test — generator=10 → writer=2
# ============================================================================
total=$((total + 1))
echo "========================================================================"
echo "  Partition test: generator partition=10 → writer partition=2"
echo "------------------------------------------------------------------------"
rm -rf /tmp/spark-partition-output
PARTITION_JSON='{"CN":"SparkTransformerJob","appName":"partition-test","generator":{"CN":"SparkGeneratorImpl","path":"scripts/input.csv","format":"csv","partition":10},"writers":[{"CN":"SparkWriterImpl","path":"/tmp/spark-partition-output","format":"json","partition":2}]}'
output=$(SPARK_MASTER=local[1] ./run.sh run.spark -j "$PARTITION_JSON" 2>&1)
rc=$?
echo "$output" | sed 's/^/  /'
if [ $rc -eq 0 ]; then
  gen_ok=$(echo "$output" | grep -c "to 10 partitions" 2>/dev/null || true)
  wr_ok=$(echo "$output" | grep -c "from 10 to" 2>/dev/null || true)
  if [ "$gen_ok" -gt 0 ] && [ "$wr_ok" -gt 0 ]; then
    echo "  Generator→10 found, writer→2 found: OK"
  else
    echo "  FAIL (gen_msg=$gen_ok wr_msg=$wr_ok)"
    rc=1
  fi
fi
if [ $rc -eq 0 ]; then
  echo "  Result: OK"
else
  echo "  Result: NOT OK (exit code $rc)"
  failures=$((failures + 1))
fi

# ============================================================================
# Test 5: StatsWriter — showWorkerRows=6, generator partition=3
# ============================================================================
total=$((total + 1))
echo "========================================================================"
echo "  StatsWriter test: showWorkerRows=6 with 3 partitions"
echo "------------------------------------------------------------------------"
STATS_JSON='{"CN":"SparkTransformerJob","appName":"stats-test","generator":{"CN":"SparkGeneratorImpl","path":"scripts/input.csv","format":"csv","partition":3},"writers":[{"CN":"SparkStatsWriter","showRows":true,"showDebugRowsNumber":3,"showWorkerRows":6}]}'
output=$(SPARK_MASTER=local[1] ./run.sh run.spark -j "$STATS_JSON" 2>&1)
rc=$?
echo "$output" | sed 's/^/  /'
  if [ $rc -eq 0 ]; then
    count_ok=$(echo "$output" | grep -c "count 100" 2>/dev/null || true)
    worker_ok=$(echo "$output" | grep -c "requesting 6 rows" 2>/dev/null || true)
    sample_shown=$(echo "$output" | grep -c "Data sample" 2>/dev/null || true)
    if [ "$count_ok" -gt 0 ] && [ "$worker_ok" -gt 0 ] && [ "$sample_shown" -gt 0 ]; then
      echo "  count=100 found, worker rows requesting 6, data sample shown: yes: OK"
    else
      echo "  FAIL (count_ok=$count_ok worker_ok=$worker_ok sample_shown=$sample_shown)"
      rc=1
    fi
  fi
if [ $rc -eq 0 ]; then
  echo "  Result: OK"
else
  echo "  Result: NOT OK (exit code $rc)"
  failures=$((failures + 1))
fi

echo ""
echo "========================================================================"
echo "  $total test(s) | $failures failure(s)"
if [ "$failures" -eq 0 ]; then
  echo "  All OK"
else
  echo "  Some NOT OK"
  exit 1
fi
