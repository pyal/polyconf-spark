package org.polyconf.spark.core

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.spark.sql.SparkSession
import org.polyconf.spark.datasource.{SparkBqIO, SparkDataIO, SparkEsIO, SparkFileDataIO, SparkPubsubIO}
import org.polyconf.util.PolyLog

object SparkSessionInit {
  private val log = LogManager.getLogger(getClass)
  private val datasourcesRegistered = new java.util.concurrent.atomic.AtomicBoolean(false)

  def create(
      appName: String = "polyconf-spark",
      master: String = sys.env.getOrElse("SPARK_MASTER", "local[*]"),
      config: Map[String, String] = Map.empty
  ): SparkSession = {
    log.info(s"Starting SparkSession: appName=$appName master=$master")

    var builder = SparkSession.builder().appName(appName)
    if (master.nonEmpty) builder = builder.master(master)

    builder = builder.config("spark.polyconf.logRules", PolyLog.currentLogRules)
    for ((k, v) <- config) builder = builder.config(k, v)

    val configUrl = getClass.getClassLoader.getResource("log4j2.xml")
    if (configUrl != null) {
      System.setProperty("log4j2.configurationFile", configUrl.toString)
    }

    val spark = builder.getOrCreate()

    // Spark overrides log4j config during initialization; reapply ours
    if (configUrl != null) {
      val ctx = LoggerContext.getContext(false)
      ctx.setConfigLocation(configUrl.toURI)
      ctx.reconfigure()
    }

    spark.sparkContext.setLogLevel("WARN")

    SparkLogRelay.init(spark.sparkContext)

    if (datasourcesRegistered.compareAndSet(false, true)) {
      SparkDataIO.register(SparkFileDataIO("text"))
      SparkDataIO.register(SparkFileDataIO("parquet"))
      SparkDataIO.register(SparkFileDataIO("avro"))
      SparkDataIO.register(SparkFileDataIO("json"))
      SparkDataIO.register(SparkFileDataIO("csv"))
      SparkDataIO.register(new SparkBqIO)
      SparkDataIO.register(new SparkEsIO)
      SparkDataIO.register(SparkFileDataIO("delta"))
      SparkDataIO.register(new SparkPubsubIO)
    }

    log.info("SparkSession created, datasources registered")
    spark
  }

  def stop(spark: SparkSession): Unit = {
    SparkLogRelay.printMasterLogs()
    spark.stop()
  }
}
