package org.polyconf.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

import scala.collection.concurrent.TrieMap

object SparkAccStore {
  private val sparkAccs = TrieMap.empty[String, LongAccumulator]

  def register(name: String, sc: SparkContext): LongAccumulator =
    sparkAccs.getOrElseUpdate(name, sc.longAccumulator(name))

  def getValue(name: String): Option[Long] =
    sparkAccs.get(name).map(_.value)

  def clear(): Unit =
    sparkAccs.clear()
}
