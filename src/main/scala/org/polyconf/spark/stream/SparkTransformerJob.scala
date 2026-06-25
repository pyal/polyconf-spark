package org.polyconf.spark.stream

import org.apache.logging.log4j.LogManager
import org.polyconf.cli.stream.{StreamGenerator, StreamTransformer, StreamWriter, SuccessRecorderBase, TransformerJob}
import org.polyconf.spark.core.{CliSparkInit, SparkLogRelay}

import scala.util.Try

final case class SparkTransformerJob(
    override val sparkConfig: Map[String, String] = Map.empty,
    override val appName: String = "polyconf-spark-job",
    override val master: String = "local[*]"
) extends TransformerJob[DFData] with CliSparkInit {

  override def help: String =
    """
      |SparkTransformerJob runs a data pipeline on Spark: generator -> transformers -> writers.
      |T is DFData (wraps Spark DataFrame with options).
       |generator: reads input as DataFrames (SparkGeneratorImpl, StreamDataGenerator, etc.)
       |transformers: DataFrame-native transformers (SparkBasicTransformer with sqlFilter, selectColumns)
       |writers: output results (SparkWriterImpl, StreamDataWriter, etc.)
      |successRecorderOpt: optional success recorder for tracking processed files
      |clearStatus: clear success recorder on init (default: false)
      |appName: Spark app name (default: polyconf-spark-job)
      |master: Spark master URL (default: local[*])
      |sparkConfig: additional Spark config key-value pairs
      |""".stripMargin

  override def run(): String = {
    val log = LogManager.getLogger(getClass)
    log.info(s"SparkTransformerJob starting: appName=$appName master=$master")
    val spark = createSpark()
    val result = Try {
      super.run()
    }.recover { case e: Exception =>
      log.error("SparkTransformerJob failed", e)
      s"FAIL: ${e.getMessage}"
    }
    stopSpark(spark)
    result.get
  }
}
