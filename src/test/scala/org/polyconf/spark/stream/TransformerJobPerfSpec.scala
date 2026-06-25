package org.polyconf.spark.stream

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.polyconf.cli.stream.{StreamGenerator, StreamDataEnvelope, TransformerJob}
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class TransformerJobPerfSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("perf-test")
      .master(TestSparkMaster.url)
      .config(TestSparkMaster.config)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  val testRows = 100

  "TransformerJob perf" should "track genRows and written for generator-only pipeline" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 5)
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq.empty, Seq(wr))
    job.runTransformers.get
    job.perf.get("filesRead").count shouldBe 1
    job.perf.get("genRows").count shouldBe testRows
    job.perf.get("written").count shouldBe testRows
    job.perf.get("wr1").count shouldBe testRows
    job.perf.get("wr1").timeNanos should be > 0L
    job.perf.get("totalTime").count shouldBe testRows
    job.perf.get("totalTime").timeNanos should be > 0L
  }

  it should "track tr1 after single filter transformer" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 10)
    val tx = TestDataTransformer(filterExpr = "id >= 50")
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq(tx), Seq(wr))
    job.runTransformers.get
    job.perf.get("genRows").count shouldBe testRows
    job.perf.get("tr1").count shouldBe 50
    job.perf.get("written").count shouldBe 50
    job.perf.get("wr1").count shouldBe 50
  }

  it should "track tr1 and tr2 after two transformers" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 10)
    val tx1 = TestDataTransformer(filterExpr = "id >= 50")
    val tx2 = TestDataTransformer(filterExpr = "id <= 80")
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq(tx1, tx2), Seq(wr))
    job.runTransformers.get
    job.perf.get("genRows").count shouldBe testRows
    job.perf.get("tr1").count shouldBe 50  // id >= 50
    job.perf.get("tr2").count shouldBe 31  // id >= 50 AND id <= 80 → 50..80
    job.perf.get("written").count shouldBe 31
    job.perf.get("wr1").count shouldBe 31
  }

  it should "track wr1 and wr2 with two writers" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 10)
    val tx = TestDataTransformer(filterExpr = "id >= 50")
    val wr1 = TestDataWriter()
    val wr2 = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq(tx), Seq(wr1, wr2))
    job.runTransformers.get
    job.perf.get("genRows").count shouldBe testRows
    job.perf.get("tr1").count shouldBe 50
    job.perf.get("wr1").count shouldBe 50
    job.perf.get("wr1").timeNanos should be > 0L
    job.perf.get("wr2").count shouldBe 50
    job.perf.get("wr2").timeNanos should be > 0L
    job.perf.get("written").count shouldBe 50
  }

  it should "handle zero-row pipeline" in {
    val gen = TestDataGenerator(numRows = 0, numPartitions = 1)
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq.empty, Seq(wr))
    job.runTransformers.get
    job.perf.get("genRows").count shouldBe 0
    job.perf.get("written").count shouldBe 0
    job.perf.get("wr1").count shouldBe 0
  }

  it should "track filter producing zero rows" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 5)
    val tx = TestDataTransformer(filterExpr = "id < 0")
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq(tx), Seq(wr))
    job.runTransformers.get
    job.perf.get("genRows").count shouldBe testRows
    job.perf.get("tr1").count shouldBe 0
    job.perf.get("written").count shouldBe 0
    job.perf.get("wr1").count shouldBe 0
  }

  it should "accumulate rows across multiple input files" in {
    val gen = MultiFileTestDataGenerator(numRows = 30, filesCount = 3)
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq.empty, Seq(wr))
    job.runTransformers.get
    job.perf.get("filesRead").count shouldBe 3
    job.perf.get("genRows").count shouldBe 90
    job.perf.get("written").count shouldBe 90
    job.perf.get("wr1").count shouldBe 90
  }

  it should "have entries containing filesRead, genRows, written, wr1, totalTime" in {
    val gen = TestDataGenerator(numRows = 50, numPartitions = 5)
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq.empty, Seq(wr))
    job.runTransformers.get
    val keys = job.perf.entries.keySet
    keys should contain("filesRead")
    keys should contain("genRows")
    keys should contain("written")
    keys should contain("wr1")
    keys should contain("totalTime")
  }

  it should "reject inconsistent stage counts without transformers" in {
    val gen = TestDataGenerator(numRows = testRows, numPartitions = 10)
    val tx = TestDataTransformer(filterExpr = "id >= 50")
    val wr = TestDataWriter(targetPartitions = 1)
    val job = TransformerJob[DFData](gen, Seq(tx), Seq(wr))
    job.runTransformers.get
    val genCount = job.perf.get("genRows").count
    val tr1Count = job.perf.get("tr1").count
    genCount should be > tr1Count
  }
}

case class MultiFileTestDataGenerator(numRows: Int, filesCount: Int)
    extends StreamGenerator[DFData] {
  override def readIterator: Iterator[StreamDataEnvelope[DFData]] = {
    val spark = SparkSession.builder().getOrCreate()
    (0 until filesCount).iterator.map { idx =>
      val data = (0 until numRows).map { i => Row(i, s"f${idx}g${i % 2}") }
      val rdd = spark.sparkContext.parallelize(data, 2)
      val df = spark.createDataFrame(rdd, schema)
      StreamDataEnvelope(Try(DFData(df)), s"file-$idx")
    }
  }
  private val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("group", StringType),
  ))
}
