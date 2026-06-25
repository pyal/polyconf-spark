package org.polyconf.spark.datasource

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.polyconf.cli.stream.WriteMode

class SparkFileDataIO(val formatName: String) extends SparkDataIO {
  require(Set("avro", "parquet", "text", "json", "csv", "delta").contains(formatName), s"Unsupported format: $formatName")

  override def name: String = formatName

  override def help: String =
    s"""
      |$formatName file datasource: reads/writes local/HDFS files in $formatName format.
      |Reader Options: any Spark DataFrameReader option (passed through)
      |Writer Options: any Spark DataFrameWriter option (passed through)
      |removeStorage: deletes file path from filesystem (no-op for delta)
      |""".stripMargin

  private val csvOpts: Seq[(String, String)] = Seq(("header", "true"), ("quote", "\""), ("escape", "\\"))

  override def reader(
      spark: SparkSession,
      path: String,
      options: Map[String, String] = Map.empty
  ): DataFrame = {
    val optSeq = formatName match {
      case "csv" => Seq(("inferSchema", "true")) ++ csvOpts
      case _     => Seq.empty
    }
    val reader = spark.read.format(formatName)
    optSeq.foldLeft(reader) { case (r, (k, v)) => r.option(k, v) }
      .options(options)
      .load(path)
  }

  override def writer(
      df: DataFrame,
      path: String,
      mode: WriteMode = WriteMode.Overwrite,
      options: Map[String, String] = Map.empty
  ): Unit = {
    if (mode == WriteMode.Overwrite)
      removeStorage(path, df.sparkSession, options)
    val optSeq = formatName match {
      case "csv"  => csvOpts
      case "text" => Seq.empty
      case _      => Seq.empty
    }
    val dfOut = formatName match {
      case "text" =>
        df.withColumn("jsonString", to_json(struct(df.columns.toIndexedSeq.map(col): _*)))
          .select("jsonString")
      case _ => df
    }
    optSeq.foldLeft(dfOut.write.mode(mode.toString).format(formatName)) {
      case (w, (k, v)) => w.option(k, v)
    }.options(options)
      .save(path)
  }

  override def removeStorage(storageName: String, sparkSession: SparkSession, options: Map[String, String]): Unit = {
    if (formatName == "delta") return
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val outPath = new Path(storageName)
    if (fs.exists(outPath)) fs.delete(outPath, true)
  }
}

object SparkFileDataIO {
  def apply(formatName: String): SparkFileDataIO = new SparkFileDataIO(formatName)
}
