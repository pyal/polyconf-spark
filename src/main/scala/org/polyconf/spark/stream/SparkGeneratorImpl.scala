package org.polyconf.spark.stream

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.polyconf.cli.stream.{StreamGenerator, FileSource, StreamDataEnvelope}

import java.io.File
import scala.util.Try

class SparkGeneratorImpl extends StreamGenerator[DFData]
    with FileSource {

  val partition: Int = -1

  @transient
  private lazy val log = LogManager.getLogger(getClass)

  override def readIterator: Iterator[StreamDataEnvelope[DFData]] = {
    val spark = SparkSession.builder().getOrCreate()
    val file = new File(path)
    if (file.isDirectory) {
      Option(file.listFiles()).getOrElse(Array.empty).filter(_.isFile).sortBy(_.getName).iterator.map { f =>
        buildFrame(spark, f.getPath, f.getName)
      }
    } else {
      Iterator(buildFrame(spark, path, file.getName))
    }
  }

  private def buildFrame(spark: SparkSession, p: String, label: String) = {
    import org.polyconf.spark.datasource.SparkDataIO
    val src = Try(SparkDataIO.get(format)).toOption
    val df = src match {
      case Some(ds) => ds.reader(spark, p, options)
      case None     =>
        log.warn(s"Format '$format' not registered as a SparkDataIO, falling back to generic Spark DataSource API")
        var dfr = spark.read.format(format)
        for ((k, v) <- options) dfr = dfr.option(k, v)
        dfr.load(p)
    }
    StreamDataEnvelope(Try(DFData(repartitionIfNeeded(df), options)), p)
  }

  private def repartitionIfNeeded(df: DataFrame): DataFrame = {
    if (partition > 0) {
      val current = df.rdd.getNumPartitions
      if (current != partition) {
        log.info(s"Repartitioning from $current to $partition partitions")
        df.repartition(partition)
      } else {
        log.info(s"DataFrame already has $partition partitions, no repartition needed")
        df
      }
    } else {
      log.info(s"partition=$partition (<=0), keeping current partitioning")
      df
    }
  }

  override def help: String =
    s"""
       |Spark DataFrame generator.
       |Reads files/directories as DataFrames using Spark DataSource API.
       |path: file or directory path
       |format: one of ${org.polyconf.spark.datasource.SparkDataIO.available.mkString(", ")} or any Spark format
       |partition: target partitions (<=0 = keep current, -1 = default)
       |options: Spark read options (format-specific)
       |""".stripMargin
}
