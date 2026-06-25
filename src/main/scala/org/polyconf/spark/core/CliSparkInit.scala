package org.polyconf.spark.core

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.polyconf.util.PolyLog

/** Spark session lifecycle mixin.
  * Fields are concrete vals with defaults (like FileSource pattern) so that
  * subclasses do not need to redeclare them — JSON deserialization sets them
  * via field injection. Override only to change defaults. */
trait CliSparkInit {
  val sparkConfig: Map[String, String] = Map.empty
  val appName: String = "polyconf-spark"
  val master: String = "local[*]"

  @transient
  protected lazy val log = LogManager.getLogger(getClass)

  protected def createSpark(): SparkSession = {
    val spark = SparkSessionInit.create(appName, master, sparkConfig)
    PolyLog.setLogRules(PolyLog.currentLogRules)
    log.info(s"CliSparkInit spark session created: appName=$appName master=$master")
    spark
  }

  protected def stopSpark(spark: SparkSession): Unit = {
    SparkSessionInit.stop(spark)
  }
}
