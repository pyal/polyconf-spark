package org.polyconf.spark.stream

import org.apache.spark.sql.DataFrame
import org.apache.spark.util.LongAccumulator
import org.polyconf.cli.stream.StreamDataBase

case class DFData(
    df: DataFrame,
    options: Map[String, Any] = Map.empty
) extends StreamDataBase {

  def countPass: (StreamDataBase, () => Long) = {
    val spark = df.sparkSession
    val acc = spark.sparkContext.longAccumulator("rowCount-" + java.util.UUID.randomUUID().toString)
    val countedDf = spark.createDataFrame(
      df.rdd.map { row => acc.add(1L); row },
      df.schema
    )
    (DFData(countedDf, options), () => acc.value)
  }

  def persist: this.type = {
    df.cache()
    this
  }

  def unpersist: this.type = {
    df.unpersist()
    this
  }
}
