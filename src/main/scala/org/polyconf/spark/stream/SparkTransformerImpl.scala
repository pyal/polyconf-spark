package org.polyconf.spark.stream

import org.polyconf.cli.stream.{StreamTransformer, StreamDataEnvelope}
import org.polyconf.spark.core.SparkLogRelay

import scala.util.Try

final case class SparkBasicTransformer(
    sqlFilter: String = "",
    selectColumns: Seq[String] = Seq.empty,
    repartition: Int = -1,
    cacheEnabled: Boolean = false
) extends StreamTransformer[DFData] {

  override def transform(input: StreamDataEnvelope[DFData]): Iterator[StreamDataEnvelope[DFData]] = {
    val result = input.mapData { frameData =>
      var df = frameData.df
      if (selectColumns.nonEmpty) df = df.select(selectColumns.head, selectColumns.tail: _*)
      if (sqlFilter.nonEmpty) df = df.filter(sqlFilter)
      if (repartition > 0) df = df.repartition(repartition)
      if (cacheEnabled) df = df.cache()
      DFData(df, frameData.options)
    }
    Iterator(result)
  }

  override def help: String =
    """
      |Spark DataFrame transformer.
      |Applies DataFrame operations natively (no StreamData conversion).
      |selectColumns: columns to keep (default: all)
      |sqlFilter: SQL filter expression (e.g. "age >= 18")
      |repartition: repartition to N partitions (0 = no repartition)
      |cacheEnabled: cache DataFrame after transform (default: false)
      |""".stripMargin
}
