package org.polyconf.spark.core

import org.apache.spark.util.CollectionAccumulator
import org.scalatest.BeforeAndAfterAll
import scala.jdk.CollectionConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkLogRelaySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: Option[org.apache.spark.sql.SparkSession] = None

  override def afterAll(): Unit = {
    spark.foreach(_.stop())
    SparkLogRelay.clear()
  }

  "SparkLogRelay" should "provide a logger" in {
    val log = SparkLogRelay.getLogger("test.logger")
    log should not be null
    log.getName shouldBe "test.logger"
  }

  it should "capture log messages and flush to explicit accumulator" in {
    SparkLogRelay.clear()
    val acc = new CollectionAccumulator[String]
    val log = SparkLogRelay.getLogger("flush.test")
    log.warn("hello from flush test")
    SparkLogRelay.flushWorkerLogs(acc)
    acc.value.size() shouldBe 1
    acc.value.get(0) should include("hello from flush test")
  }

  it should "accumulate multiple log messages via repeated flushes" in {
    SparkLogRelay.clear()
    val acc = new CollectionAccumulator[String]

    val log = SparkLogRelay.getLogger("multi.test")
    log.warn("msg one")
    log.warn("msg two")
    SparkLogRelay.flushWorkerLogs(acc)
    acc.value.size() shouldBe 2

    log.warn("msg three")
    SparkLogRelay.flushWorkerLogs(acc)
    acc.value.size() shouldBe 3
  }

  it should "flushed logs appear in getMasterLogs after driver-side flush" in {
    SparkLogRelay.clear()
    val acc = new CollectionAccumulator[String]
    val log = SparkLogRelay.getLogger("master.test")
    log.warn("master visible")
    SparkLogRelay.flushWorkerLogs(acc)

    val logs = acc.value.asScala.toSeq
    logs.exists(_.contains("master visible")) shouldBe true
  }

  it should "handle clear" in {
    SparkLogRelay.getLogger("clear.test").warn("before clear")
    SparkLogRelay.clear()
    SparkLogRelay.getMasterLogs shouldBe empty
  }

  it should "use configured log level from driver on workers" in {
    val s = SparkSessionInit.create("log-level-test", master = "local[2]",
      config = Map("spark.polyconf.logRules" -> "INFO"))
    spark = Some(s)
    val sc = s.sparkContext

    val acc = new CollectionAccumulator[String]
    sc.register(acc, "level-test")

    sc.parallelize(Seq("msg"), 1).mapPartitions { iter =>
      val log = SparkLogRelay.getLogger("worker.level")
      log.info("info level msg")
      log.debug("debug level msg")
      SparkLogRelay.flushWorkerLogs(acc)
      iter
    }.collect()

    val logs = acc.value.asScala.toSeq
    logs.count(_.contains("info level msg")) shouldBe 1
    logs.count(_.contains("debug level msg")) shouldBe 0
  }
}
