package org.polyconf.spark.datasource

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.polyconf.cli.stream.WriteMode

import scala.collection.concurrent.TrieMap

trait SparkDataIO {
  def name: String
  def reader(
      spark: SparkSession,
      path: String,
      options: Map[String, String] = Map.empty
  ): DataFrame
  def writer(
      df: DataFrame,
      path: String,
      mode: WriteMode = WriteMode.Overwrite,
      options: Map[String, String] = Map.empty
  ): Unit
  def removeStorage(storageName: String, sparkSession: SparkSession, options: Map[String, String] = Map.empty): Unit = {
    throw new RuntimeException(s"removeStorage not implemented for $name")
  }
  def help: String = "No help"
}

object SparkDataIO {
  private val log = LogManager.getLogger(getClass)

  private val registry = TrieMap.empty[String, SparkDataIO]

  def register(source: SparkDataIO): Unit = {
    registry(source.name) = source
    log.info(s"Registered SparkDataIO: ${source.name}")
  }

  def get(name: String): SparkDataIO =
    registry.getOrElse(name, throw new NoSuchElementException(s"Unknown SparkDataIO: $name. Available: ${registry.keys.mkString(", ")}"))

  def available: Set[String] = registry.keySet.toSet

  def helpAll: String =
    registry.values.foldLeft("Available SparkDataIO datasources:\n") { case (acc, io) =>
      acc + s"\n  ${io.name}:\n${io.help.stripPrefix("\n")}\n"
    }
}
