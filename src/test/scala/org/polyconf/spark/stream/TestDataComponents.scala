package org.polyconf.spark.stream

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.polyconf.cli.stream.{StreamGenerator, StreamTransformer, StreamWriter, StreamDataEnvelope}

import scala.util.Try

final case class TestDataGenerator(
    numRows: Int = 100,
    numPartitions: Int = 10
) extends StreamGenerator[DFData] {

  override def readIterator: Iterator[StreamDataEnvelope[DFData]] = {
    val spark = SparkSession.builder().getOrCreate()
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("group", StringType),
    ))
    val data = (0 until numRows).map { i =>
      Row(i, s"g${i % (numPartitions * 2)}")
    }
    val rdd = spark.sparkContext.parallelize(data, numPartitions)
    val df = spark.createDataFrame(rdd, schema)
    Iterator(StreamDataEnvelope(Try(DFData(df)), "test-input"))
  }
}

final case class TestDataTransformer(
    filterExpr: String = ""
) extends StreamTransformer[DFData] {

  override def transform(input: StreamDataEnvelope[DFData]): Iterator[StreamDataEnvelope[DFData]] = {
    val result = input.mapData { fd =>
      val df = if (filterExpr.nonEmpty) fd.df.filter(filterExpr) else fd.df
      DFData(df, fd.options)
    }
    Iterator(result)
  }
}

final case class TestDataWriter(
    targetPartitions: Int = 0
) extends StreamWriter[DFData] {

  override def write(input: StreamDataEnvelope[DFData]): Try[Unit] = Try {
    input.data.foreach { fd =>
      val df = if (targetPartitions > 0) fd.df.repartition(targetPartitions) else fd.df
      df.write.mode("overwrite").format("noop").save()
    }
  }
}
