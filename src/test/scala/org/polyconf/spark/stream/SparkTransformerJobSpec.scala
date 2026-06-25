package org.polyconf.spark.stream

import org.polyconf.core.{PolyConf, PolyConfRegistry}
import org.polyconf.spark.TestSparkMaster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkTransformerJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    PolyConfRegistry.registerAll()
  }

  private def sparkGenerator(path: String, format: String): SparkGeneratorImpl =
    PolyConf.deserialize[SparkGeneratorImpl](
      s"""{"CN":"SparkGeneratorImpl","path":"$path","format":"$format"}""",
      verify = false
    )

  "SparkTransformerJob" should "construct with a SparkGenerator" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val genJson = PolyConf.serialize(gen)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.generator should not be null
  }

  it should "construct with an empty transform list" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val genJson = PolyConf.serialize(gen)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.transformers shouldBe empty
  }

  it should "construct with one transformer" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val tr = SparkBasicTransformer(sqlFilter = "age > 18")
    val genJson = PolyConf.serialize(gen)
    val trJson = PolyConf.serialize(tr)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson,"transformers":[$trJson]}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.transformers should have size 1
  }

  it should "construct with multiple transformers" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val tr1 = SparkBasicTransformer(selectColumns = Seq("name"))
    val tr2 = SparkBasicTransformer(sqlFilter = "age > 18")
    val genJson = PolyConf.serialize(gen)
    val tr1Json = PolyConf.serialize(tr1)
    val tr2Json = PolyConf.serialize(tr2)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson,"transformers":[$tr1Json,$tr2Json]}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.transformers should have size 2
  }

  it should "construct with a writer" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val wr = PolyConf.deserializeAny[SparkWriterImpl]("""{path:"/tmp/out",format:"parquet"}""")
    val genJson = PolyConf.serialize(gen)
    val wrJson = PolyConf.serialize(wr)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson,"writers":[$wrJson]}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.writers should have size 1
    runner.writers.head shouldBe a[SparkWriterImpl]
    val sw = runner.writers.head.asInstanceOf[SparkWriterImpl]
    sw.path shouldBe "/tmp/out"
    sw.format shouldBe "parquet"
  }

  it should "construct with StreamDataGenerator wrapper" in {
    import org.polyconf.cli.stream.SimpleDataGenerator
    val inner = SimpleDataGenerator(rowsPerFile = 1)
    val gen = StreamDataGenerator(inner)
    val genJson = PolyConf.serialize(gen)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.generator should not be null
  }

  it should "have correct Spark config fields" in {
    val gen = sparkGenerator("/tmp/test.csv", "csv")
    val genJson = PolyConf.serialize(gen)
    val json = s"""{"CN":"SparkTransformerJob","generator":$genJson,"appName":"test-job","master":"${TestSparkMaster.url}"}"""
    val runner = PolyConf.deserialize[SparkTransformerJob](json, verify = false)
    runner.appName shouldBe "test-job"
    runner.master shouldBe TestSparkMaster.url
  }
}
