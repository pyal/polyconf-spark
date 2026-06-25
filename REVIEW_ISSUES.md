# Code Review Issues — polyconf-spark

## Status Legend
- ✅ **Fixed** — fix applied and tested
- ⏭️ **Skipped** — user chose to skip
- ⬜ **Pending** — not yet addressed

---

### CRITICAL

**S1** — ✅ **Fixed** — `SparkTransformerJob` discards `super.run()` return value
- **File:** `src/main/scala/org/polyconf/spark/stream/SparkTransformerJob.scala:33-34`
- **Problem:** `Try { super.run(); "OK" }` — hardcoded `"OK"`, ignores actual return value from `TransformerJob.run()`.
- **Fix:** `Try { super.run() }` — use the actual return value.

**S2** — ✅ **Fixed** — `StreamDataAdapter` collects entire DataFrame to driver
- **File:** `src/main/scala/org/polyconf/spark/stream/StreamDataAdapter.scala:99-114`
- **Problem:** `StreamDataTransformer.transform` calls `fromDataFrame` which calls `df.collect()`, pulling all rows into driver memory. Defeats distributed processing.
- **Fix:** Added configurable `truncationLength: Int = 1000000` parameter to `fromDataFrame`, `StreamDataTransformer`, and `StreamDataWriter`. Default raised from 10000 to 1M.

**S3** — ✅ **Fixed** — `SparkBqIO` redundant `DROP TABLE` before write creates data-loss window
- **File:** `src/main/scala/org/polyconf/spark/datasource/SparkBqIO.scala:60-61`
- **Problem:** Manual `DROP TABLE IF EXISTS` before BigQuery write — if write fails after DROP, table is permanently lost. Connector's `mode("overwrite")` already handles this atomically via `WRITE_TRUNCATE`.
- **Fix:** Removed the `if (mode == WriteMode.Overwrite) removeStorage(...)` block.

**S4** — ⏭️ **Skipped** — `SparkPubsubIO.readMessages` unbounded
- **File:** `src/main/scala/org/polyconf/spark/datasource/SparkPubsubIO.scala:80-81`
- **Problem:** `PullRequest` builder never sets `maxMessages` when option absent → PubSub Pull API may return thousands of messages, all collected into driver memory.
- **Fix:** `builder.setMaxMessages(maxMessages.getOrElse(1000))`.

---

### HIGH

**S5** — ✅ **Fixed** — `fromDataFrame` does two full Spark actions
- **File:** `src/main/scala/org/polyconf/spark/stream/StreamDataAdapter.scala:24-38`
- **Problem:** `df.count()` + `df.limit(n).collect()` — two full scans instead of one.
- **Fix:** Single `df.limit(truncationLength + 1).collect()`, detect truncation from result size.

**S6** — ✅ **Fixed** — `SparkStatsWriter` never calls `df.unpersist()`
- **File:** `src/main/scala/org/polyconf/spark/stream/SparkWriterImpl.scala:73`
- **Problem:** `df.cache()` without `unpersist()` — cached data occupies executor memory until SparkContext stops.
- **Fix:** Wrapped body in `try { ... } finally { df.unpersist() }`.

---

### MEDIUM

**S7** — ⬜ **Pending** — `SparkWriterImpl` ignores `frameData.options`
- **File:** `src/main/scala/org/polyconf/spark/stream/SparkWriterImpl.scala:26`
- **Problem:** `ds.writer(dfOut, path, mode, options)` uses `this.options`, ignores per-file `frameData.options` carried in `DFData`.
- **Fix:** Merge or use `frameData.options ++ options`.

**S8** — ⬜ **Pending** — `DFData.countPass` leaks accumulators
- **File:** `src/main/scala/org/polyconf/spark/stream/DFData.scala:14`
- **Problem:** Creates new `LongAccumulator` with UUID name on every call. Accumulators registered with `SparkContext` are never cleaned up.
- **Fix:** Reuse a single accumulator or provide explicit cleanup.

**S9** — ⬜ **Pending** — `SparkPubsubIO.acknowledgeMessages` swallows exceptions
- **File:** `src/main/scala/org/polyconf/spark/datasource/SparkPubsubIO.scala:121-123`
- **Problem:** Failed acks logged as warnings but not propagated → messages redelivered, duplicate processing.
- **Fix:** Propagate exceptions or aggregate failure counts.

**S10** — ⬜ **Pending** — `SparkPubsubIO.writeMessages` no `awaitTermination`
- **File:** `src/main/scala/org/polyconf/spark/datasource/SparkPubsubIO.scala:103`
- **Problem:** `publisher.shutdown()` is non-blocking; resources not released when `foreachPartition` returns.
- **Fix:** Add `publisher.awaitTermination(30, TimeUnit.SECONDS)`.

**S11** — ⬜ **Pending** — `SparkPubsubIO.writer` ignores `mode` parameter
- **File:** `src/main/scala/org/polyconf/spark/datasource/SparkPubsubIO.scala:41`
- **Problem:** `mode` accepted but unused (all messages published, no overwrite/append distinction for PubSub).
- **Fix:** Document or remove the parameter.

**S12** — ⬜ **Pending** — `SparkCtxStorage` unhelpful `ClassCastException`
- **File:** `src/main/scala/org/polyconf/spark/core/SparkCtxStorage.scala:10`
- **Problem:** `asInstanceOf[Serializable]` — error message says nothing about which key/value.
- **Fix:** `require(value.isInstanceOf[Serializable], s"Value for key '$key' is not Serializable")`.

---

### LOW

**S13** — ✅ **Fixed** — `printMasterLogs()` called twice
- **File:** `src/main/scala/org/polyconf/spark/stream/SparkTransformerJob.scala:39`
- **Problem:** `printMasterLogs()` called directly AND via `stopSpark(spark)` → `SparkSessionInit.stop` → `SparkLogRelay.printMasterLogs()`.
- **Fix:** Removed the redundant direct call.

**S14** — ⬜ **Pending** — `SparkPolyConfProvider` registration repeats on every instance
- **File:** `src/main/scala/org/polyconf/spark/core/SparkPolyConfProvider.scala:8`
- **Problem:** If multiple instances are created, `registerAllChildForBases` runs again.
- **Fix:** Guard with a static boolean flag.

**S15** — ⬜ **Pending** — Datasources re-registered on every `SparkSessionInit.create()`
- **File:** `src/main/scala/org/polyconf/spark/core/SparkSessionInit.scala:43-51`
- **Problem:** Redundant `register` calls on every `create()` (harmless — TrieMap overwrite).
- **Fix:** Guard with a boolean flag.

**S16** — ⬜ **Pending** — `SparkLogRelay.init()` called multiple times leaks accumulator
- **File:** `src/main/scala/org/polyconf/spark/core/SparkLogRelay.scala:29-33`
- **Problem:** Each `init()` call creates a new `CollectionAccumulator` and registers it; old one remains in `SparkContext`.
- **Fix:** Guard with `if (logAcc.isEmpty)`.

---

### ADDITIONAL REVIEW NOTES

- `SparkEsIO.scala`: Similar redundant `removeStorage` before write as SparkBqIO (data-loss window). Consider fixing if ES connector supports `mode("overwrite")`.
- `SparkEsIO.scala`: Uses string literal `"UTF-8"` instead of `StandardCharsets.UTF_8` (line 102-103).
- `SparkGeneratorImpl.scala:22`: `file.listFiles()` returns `null` on I/O error → silent empty iterator. Consider logging a warning.
- `StreamDataAdapter.scala:17`: Schema inferred from only first row — downstream rows with additional keys are silently dropped.
