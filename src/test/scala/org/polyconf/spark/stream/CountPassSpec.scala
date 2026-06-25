package org.polyconf.spark.stream

import org.apache.spark.sql.SparkSession
import org.polyconf.cli.stream.{StageCount, StreamDataEnvelope, TransformerJob}
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class CountPassSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var session: SparkSession = _

  override def beforeAll(): Unit = {
    session = SparkSession.builder()
      .appName("countpass-test")
      .master(TestSparkMaster.url)
      .config(TestSparkMaster.config)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (session != null) session.stop()
  }

  "countPass" should "return correct count after single write" in {
    val df = session.range(50).repartition(5).toDF("id")
    val fd = DFData(df)
    val (counted, countFn) = fd.countPass
    countFn() shouldBe 0L
    counted.asInstanceOf[DFData].df.write.mode("overwrite").format("noop").save()
    countFn() shouldBe 50L
  }

  it should "return correct count with multi-partition data" in {
    val df = session.range(97).repartition(7).toDF("id")
    val fd = DFData(df)
    val (counted, countFn) = fd.countPass
    counted.asInstanceOf[DFData].df.write.mode("overwrite").format("noop").save()
    countFn() shouldBe 97L
  }

  it should "chain countPass through generator and transformer" in {
    val gen = TestDataGenerator(numRows = 100, numPartitions = 10)
    val tx = TestDataTransformer(filterExpr = "id >= 50")

    var stageCounts = List.empty[StageCount]

    val td = gen.readIterator.next()
    val (genCounted, genFn) = td.data.get.countPass
    stageCounts = StageCount(td.inputPath, "genRows", genFn) :: stageCounts

    val genData = td.mapData(_ => genCounted.asInstanceOf[DFData])
    val result = tx.transform(genData).next()
    val (txCounted, txFn) = result.data.get.countPass
    stageCounts = StageCount(result.inputPath, "tr1", txFn) :: stageCounts

    val finalData = result.mapData(_ => txCounted.asInstanceOf[DFData])
    finalData.data.get.df.write.mode("overwrite").format("noop").save()

    stageCounts.find(_.stage == "genRows").map(_.count()) shouldBe Some(100L)
    stageCounts.find(_.stage == "tr1").map(_.count()) shouldBe Some(50L)
  }

  it should "count correctly with persisting for multiple writers" in {
    val df = session.range(30).repartition(3).toDF("id")
    val fd = DFData(df)
    val (counted, countFn) = fd.countPass
    val countedFd = counted.asInstanceOf[DFData]

    val persisted = countedFd.persist
    persisted.df.write.mode("overwrite").format("noop").save()
    persisted.df.write.mode("overwrite").format("noop").save()
    persisted.unpersist

    countFn() shouldBe 30L
  }

  it should "work end-to-end with TransformerJob" in {
    val gen = TestDataGenerator(numRows = 100, numPartitions = 10)
    val tx = TestDataTransformer(filterExpr = "id >= 20")
    val wr = TestDataWriter()
    val job = TransformerJob[DFData](gen, Seq(tx), Seq(wr))
    val result = job.runTransformers
    result.isSuccess shouldBe true
  }
}
