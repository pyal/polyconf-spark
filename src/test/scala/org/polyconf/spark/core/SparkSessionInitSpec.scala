package org.polyconf.spark.core

import org.apache.spark.sql.SparkSession
import org.polyconf.spark.TestSparkMaster
import org.polyconf.cli.stream.WriteMode
import org.polyconf.spark.datasource.SparkDataIO
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class SparkSessionInitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    spark = SparkSessionInit.create("test", TestSparkMaster.url, TestSparkMaster.config)
    tempDir = Files.createTempDirectory("polyconf-spark-test")
  }

  override def afterAll(): Unit = {
    SparkSessionInit.stop(spark)
    tempDir.toFile.delete()
  }

  "SparkSessionInit" should "create a SparkSession" in {
    spark should not be null
    spark.version should not be empty
  }

  it should "register datasources" in {
    SparkDataIO.available should contain("json")
    SparkDataIO.available should contain("parquet")
    SparkDataIO.available should contain("avro")
    SparkDataIO.available should contain("csv")
    SparkDataIO.available should contain("text")
    SparkDataIO.available should contain("bigquery")
    SparkDataIO.available should contain("elasticsearch")
    SparkDataIO.available should contain("delta")
  }

  it should "read and write via JsonDataIO" in {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testDir = tempDir.resolve("json_test")
    val data = Seq(("Alice", 30), ("Bob", 25)).toDF("name", "age")
    data.write.mode("overwrite").json(testDir.toString)

    val ds = SparkDataIO.get("json")
    val readBack = ds.reader(spark, testDir.toString)
    readBack.count() shouldBe 2
    readBack.columns should contain("name")
    readBack.columns should contain("age")
  }

  it should "read and write via ParquetDataIO" in {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val testDir = tempDir.resolve("parquet_test")
    val data = Seq(("x", 1), ("y", 2)).toDF("k", "v")
    val ds = SparkDataIO.get("parquet")

    ds.writer(data, testDir.toString)
    val readBack = ds.reader(spark, testDir.toString)
    readBack.count() shouldBe 2
  }

  it should "support multiple WriteMode values" in {
    WriteMode.fromString("overwrite") shouldBe WriteMode.Overwrite
    WriteMode.fromString("append") shouldBe WriteMode.Append
    WriteMode.fromString("error") shouldBe WriteMode.ErrorIfExists
    WriteMode.fromString("ignore") shouldBe WriteMode.Ignore
    intercept[IllegalArgumentException] { WriteMode.fromString("unknown") }
  }
}
