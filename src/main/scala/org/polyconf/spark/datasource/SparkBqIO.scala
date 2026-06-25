package org.polyconf.spark.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.polyconf.cli.stream.WriteMode

class SparkBqIO extends SparkDataIO {
  override def name: String = "bigquery"

  override def help: String =
    """
      |BigQuery datasource: reads/writes Google BigQuery tables.
      |Reader: path format dataset.table
      |  Options: query - SQL query to read (overrides path), location - BQ location
      |Writer: path format dataset.table
      |  Options: writeMethod - "direct" (default) or "indirect" (via GCS),
      |           temporaryGcsBucket - required for indirect writes,
      |           parallelism - write parallelism
      |removeStorage: runs DROP TABLE IF EXISTS
      |""".stripMargin

  override def reader(
      spark: SparkSession,
      path: String,
      options: Map[String, String] = Map.empty
  ): DataFrame = {
    val query = options.get("query")
    require(path.nonEmpty || query.isDefined,
      "Either path (table name) or 'query' option must be provided for BigQuery reader")
    val supported = Set("location", "query", "writeMethod", "temporaryGcsBucket", "parallelism")
    val connectorOpts = options -- supported
    val location = options.get("location")

    val baseReader = spark.read.format("bigquery")
    val withLocation = location.fold(baseReader)(loc => baseReader.option("location", loc))

    query match {
      case Some(sql) =>
        var r = withLocation.option("query", sql)
        for ((k, v) <- connectorOpts) r = r.option(k, v)
        r.load()
      case None =>
        var r = withLocation.option("table", path)
        for ((k, v) <- connectorOpts) r = r.option(k, v)
        r.load()
    }
  }

  override def writer(
      df: DataFrame,
      path: String,
      mode: WriteMode = WriteMode.Overwrite,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val supported = Set("location", "query", "writeMethod", "temporaryGcsBucket", "parallelism")
    val connectorOpts = options -- supported
    val writeMethod = options.getOrElse("writeMethod", "direct")
    val gcsBucket = options.get("temporaryGcsBucket")
    val parallelism = options.get("parallelism")

    var baseWriter = df.write.format("bigquery").mode(mode.toString).option("table", path)
    parallelism.foreach(p => baseWriter = baseWriter.option("parallelism", p))
    if (writeMethod == "indirect" && gcsBucket.isDefined) {
      baseWriter = baseWriter.option("writeMethod", "indirect")
        .option("temporaryGcsBucket", gcsBucket.get)
    } else {
      baseWriter = baseWriter.option("writeMethod", "direct")
    }
    for ((k, v) <- connectorOpts) baseWriter = baseWriter.option(k, v)
    baseWriter.save()
  }

  override def removeStorage(
      storageName: String,
      sparkSession: SparkSession,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val location = options.get("location")
    val reader = sparkSession.read.format("bigquery")
    val withLocation = location.fold(reader)(loc => reader.option("location", loc))
    withLocation.option("query", s"DROP TABLE IF EXISTS `${storageName.replace("`", "``")}`").load()
  }
}
