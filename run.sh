#!/bin/bash
SPARK_JAR=./target/scala-2.13/polyconf-spark-assembly-0.1.0-SNAPSHOT.jar

# ==========================================
# Requirements
# ==========================================
# Build polyconf-spark assembly (includes polyconf):
#   sbt assembly
# ==========================================

# ==========================================
# Production: requires assembly JAR
# ==========================================

# Run a Spark job via spark-submit
run.spark() {
  if [ ! -f "$SPARK_JAR" ]; then
    echo "Spark assembly JAR not found: $SPARK_JAR" >&2
    echo "Build it first: sbt assembly" >&2
    exit 1
  fi
  local master="${SPARK_MASTER:-local[*]}"
  spark-submit --class org.polyconf.cli.run.CliRun \
    --master "$master" \
    "$SPARK_JAR" "$@"
}

# Generate CLI args from YAML config via CliArgGenerator
run.args() {
  if [ ! -f "$SPARK_JAR" ]; then
    echo "Spark assembly JAR not found: $SPARK_JAR" >&2
    echo "Build it first: sbt assembly" >&2
    exit 1
  fi
  java -cp "$SPARK_JAR" org.polyconf.argfmt.CliArgGenerator "$@"
}

# ==========================================
# Development: uses sbt (no JAR build needed)
# ==========================================

# Generate & execute from YAML config via sbt
run.dev.args() {
  sbt "runMain org.polyconf.argfmt.CliArgGenerator $*"
}

# Run a job via sbt (local Spark — polyconf-spark JAR on classpath)
run.dev.local() {
  sbt "runMain org.polyconf.cli.run.CliRun $*"
}

"$@"
