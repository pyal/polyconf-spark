package org.polyconf.spark.core

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkDistributedSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSessionInit.create("distributed-test", TestSparkMaster.url, TestSparkMaster.config)
  }

  override def afterAll(): Unit = {
    SparkSessionInit.stop(spark)
  }

  "SparkAccStore.register" should "report merged count via getSpark after distributed job" in {
    SparkAccStore.clear()
    implicit val sc: SparkContext = spark.sparkContext

    val acc = SparkAccStore.register("merged", sc)

    spark.sparkContext.parallelize(1 to 10, 2).foreach { _ =>
      acc.add(1L)
    }

    SparkAccStore.getValue("merged") shouldBe Some(10L)
  }

  it should "report multiple named accumulators after parallel execution" in {
    SparkAccStore.clear()
    implicit val sc: SparkContext = spark.sparkContext

    val cntAcc  = SparkAccStore.register("multi.cnt", sc)
    val valAcc  = SparkAccStore.register("multi.val", sc)
    val sumAcc  = SparkAccStore.register("multi.sum", sc)

    spark.sparkContext.parallelize(1 to 30, 3).foreach { i =>
      cntAcc.add(1L)
      valAcc.add(2L)
      sumAcc.add(i.toLong)
    }

    SparkAccStore.getValue("multi.cnt") shouldBe Some(30L)
    SparkAccStore.getValue("multi.val") shouldBe Some(60L)
    SparkAccStore.getValue("multi.sum") shouldBe Some(465L)
  }

  "SparkLogRelay" should "capture worker logs and promote them to master via flushWorkerLogs(acc)" in {
    SparkLogRelay.clear()
    SparkLogRelay.init(spark.sparkContext)
    val acc = SparkLogRelay.getLogAccumulator.get

    Configurator.setLevel("worker", Level.WARN)

    val data = spark.sparkContext.parallelize(Seq("alpha", "beta", "gamma"), 3)
    data.foreachPartition { iter =>
      val log = SparkLogRelay.getLogger("worker")
      iter.foreach { item =>
        log.warn(s"raw-$item")
      }
      SparkLogRelay.flushWorkerLogs(acc)
    }

    val masterLogs = SparkLogRelay.getMasterLogs
    masterLogs should have size 3
    masterLogs.exists(_.contains("raw-")) shouldBe true
  }

  it should "capture logs from multiple named loggers across partitions" in {
    SparkLogRelay.clear()
    SparkLogRelay.init(spark.sparkContext)
    val acc = SparkLogRelay.getLogAccumulator.get

    Configurator.setLevel("logger.a", Level.WARN)
    Configurator.setLevel("logger.b", Level.WARN)

    val data = spark.sparkContext.parallelize(1 to 6, 3)
    data.foreachPartition { iter =>
      val logA = SparkLogRelay.getLogger("logger.a")
      val logB = SparkLogRelay.getLogger("logger.b")
      iter.foreach { i =>
        logA.warn(s"a-$i")
        logB.warn(s"b-$i")
      }
      SparkLogRelay.flushWorkerLogs(acc)
    }

    val logs = SparkLogRelay.getMasterLogs
    logs.size shouldBe 12
    logs.count(_.contains("a-")) shouldBe 6
    logs.count(_.contains("b-")) shouldBe 6
  }

  it should "not lose logs when multiple partitions flush concurrently" in {
    SparkLogRelay.clear()
    SparkLogRelay.init(spark.sparkContext)
    val acc = SparkLogRelay.getLogAccumulator.get

    Configurator.setLevel("concurrent", Level.WARN)

    val data = spark.sparkContext.parallelize(1 to 20, 6)
    data.foreachPartition { iter =>
      val log = SparkLogRelay.getLogger("concurrent")
      iter.foreach { i =>
        if (i % 2 == 0) log.warn(s"even-$i")
        else log.warn(s"odd-$i")
      }
      SparkLogRelay.flushWorkerLogs(acc)
    }

    val logs = SparkLogRelay.getMasterLogs
    logs.count(_.contains("even-")) shouldBe 10
    logs.count(_.contains("odd-")) shouldBe 10
  }

  "SparkAccStore and SparkLogRelay" should "work together in a combined distributed pipeline" in {
    SparkAccStore.clear()
    SparkLogRelay.clear()
    SparkLogRelay.init(spark.sparkContext)
    implicit val sc: SparkContext = spark.sparkContext
    val acc = SparkLogRelay.getLogAccumulator.get

    Configurator.setLevel("pipeline", Level.WARN)

    val cntAcc = SparkAccStore.register("pipeline.cnt", sc)

    val data = spark.sparkContext.parallelize(1 to 50, 5)
    data.foreachPartition { iter =>
      val log = SparkLogRelay.getLogger("pipeline")
      var partCount = 0L
      iter.foreach { i =>
        partCount += 1
        cntAcc.add(1L)
        if (partCount % 10 == 0) log.warn(s"hit $partCount")
      }
      SparkLogRelay.flushWorkerLogs(acc)
    }

    SparkAccStore.getValue("pipeline.cnt") shouldBe Some(50L)
    val logs = SparkLogRelay.getMasterLogs
    logs.exists(_.contains("hit 10")) shouldBe true
  }
}
