package org.polyconf.spark.stream

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.polyconf.cli.stream.StreamDataEnvelope
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class SparkDataFrameSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var session: SparkSession = _

  override def beforeAll(): Unit = {
    session = SparkSession.builder()
      .appName("dataframe-test")
      .master(TestSparkMaster.url)
      .config(TestSparkMaster.config)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (session != null) session.stop()
  }

  "DFData" should "construct from DataFrame" in {
    val spark = session
    import spark.implicits._
    val df = Seq(("a", 1), ("b", 2)).toDF("k", "v")
    val fd = DFData(df, Map.empty)
    fd.df.columns should contain("k")
    fd.df.columns should contain("v")
    fd.options shouldBe empty
  }

  it should "track count via countPass accumulator after action" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "x")).toDF("n", "s")
    val fd = DFData(df, Map("key" -> "val"))
    fd.options shouldBe Map("key" -> "val")
    val (counted, countFn) = fd.countPass
    countFn() shouldBe 0L
    counted.asInstanceOf[DFData].df.count() shouldBe 1L
    countFn() shouldBe 1L
  }

  it should "support empty DataFrames" in {
    val spark = session
    val schema = StructType(Seq(
      StructField("col1", StringType, true),
      StructField("col2", IntegerType, true),
    ))
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val fd = DFData(df, Map.empty)
    fd.df.count() shouldBe 0L
  }

  it should "preserve schema through round-trip" in {
    val spark = session
    import spark.implicits._
    val df = Seq((42, "hello")).toDF("id", "msg")
    val fd = DFData(df, Map.empty)
    fd.df.schema.fieldNames should contain("id")
    fd.df.schema.fieldNames should contain("msg")
  }

  "SparkBasicTransformer" should "select columns" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "a", true), (2, "b", false)).toDF("x", "y", "z")
    val fd = DFData(df, Map.empty)
    val transformer = SparkBasicTransformer(selectColumns = Seq("x", "z"))
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = transformer.transform(input).next().data.get
    result.df.columns should contain only ("x", "z")
  }

  it should "apply SQL filter" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "cat"), (2, "dog"), (3, "cat")).toDF("id", "animal")
    val fd = DFData(df, Map.empty)
    val transformer = SparkBasicTransformer(sqlFilter = "animal = 'cat'")
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = transformer.transform(input).next().data.get
    result.df.count() shouldBe 2L
  }

  it should "repartition to configured count" in {
    val spark = session
    import spark.implicits._
    val df = (1 to 100).toDF("n").repartition(4)
    val fd = DFData(df, Map.empty)
    val transformer = SparkBasicTransformer(repartition = 6)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = transformer.transform(input).next().data.get
    result.df.rdd.getNumPartitions shouldBe 6
  }

  it should "optionally cache the DataFrame" in {
    val spark = session
    import spark.implicits._
    val df = Seq(("cached", 1)).toDF("label", "val")
    val fd = DFData(df, Map.empty)
    val transformer = SparkBasicTransformer(cacheEnabled = true)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = transformer.transform(input).next().data.get
    result.df.storageLevel.useMemory shouldBe true
  }

  it should "support combined select+filter+repartition" in {
    val spark = session
    import spark.implicits._
    val df = Seq(
      (1, "alice", 30),
      (2, "bob", 25),
      (3, "charlie", 35),
    ).toDF("id", "name", "age")
    val fd = DFData(df, Map.empty)
    val transformer = SparkBasicTransformer(
      selectColumns = Seq("name", "age"),
      sqlFilter = "age >= 30",
      repartition = 2,
    )
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = transformer.transform(input).next().data.get
    result.df.columns should contain only ("name", "age")
    result.df.count() shouldBe 2L
    result.df.rdd.getNumPartitions shouldBe 2
  }

  "StreamDataAdapter" should "convert StreamData to DataFrame and back" in {
    import org.polyconf.cli.stream.StreamData
    val spark = session
    val packet = StreamData(Seq(
      Map("name" -> "Alice", "age" -> 30),
      Map("name" -> "Bob", "age" -> 25),
    ))
    val df = StreamDataAdapter.toDataFrame(spark, packet)
    df.count() shouldBe 2L
    df.columns should contain("name")
    df.columns should contain("age")

    val back = StreamDataAdapter.fromDataFrame(df)
    back.data should have size 2
    back.data.head("name") shouldBe "Alice"
    back.data.head("age") shouldBe 30
  }

  it should "handle empty StreamData" in {
    import org.polyconf.cli.stream.StreamData
    val spark = session
    val packet = StreamData(Seq.empty)
    val df = StreamDataAdapter.toDataFrame(spark, packet)
    df.count() shouldBe 0L
  }
}
