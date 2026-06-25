package org.polyconf.spark.stream

import org.apache.spark.sql.SparkSession
import org.polyconf.cli.stream.StreamDataEnvelope
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class SparkTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var session: SparkSession = _

  override def beforeAll(): Unit = {
    session = SparkSession.builder()
      .appName("transformer-test")
      .master(TestSparkMaster.url)
      .config(TestSparkMaster.config)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (session != null) session.stop()
  }

  "SparkBasicTransformer" should "filter rows via sqlFilter" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("key", "label")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(sqlFilter = "key > 1", cacheEnabled = false)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.count() shouldBe 2L
  }

  it should "work with empty DataFrame" in {
    val spark = session
    import spark.implicits._
    val df = Seq.empty[(Int, String)].toDF("k", "v")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(sqlFilter = "k = 1", cacheEnabled = false)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.count() shouldBe 0L
  }

  it should "chain selectColumns + sqlFilter + repartition" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "x", 10), (2, "y", 20), (3, "z", 30)).toDF("id", "name", "val")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(
      selectColumns = Seq("name", "val"),
      sqlFilter = "val >= 20",
      repartition = 1,
      cacheEnabled = false,
    )
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.columns should contain only ("name", "val")
    result.df.count() shouldBe 2L
    result.df.rdd.getNumPartitions shouldBe 1
  }

  it should "leave DataFrame unchanged when no options set" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "unchanged")).toDF("a", "b")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(cacheEnabled = false)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.count() shouldBe 1L
    result.df.columns should contain only ("a", "b")
  }

  it should "cache when cacheEnabled=true" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "cached")).toDF("k", "v")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(cacheEnabled = true)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.storageLevel.useMemory shouldBe true
  }

  it should "not cache when cacheEnabled=false" in {
    val spark = session
    import spark.implicits._
    val df = Seq((1, "nocache")).toDF("k", "v")
    val fd = DFData(df, Map.empty)
    val t = SparkBasicTransformer(cacheEnabled = false)
    val input = StreamDataEnvelope(Success(fd), "test")
    val result = t.transform(input).next().data.get
    result.df.storageLevel.useMemory shouldBe false
  }
}
