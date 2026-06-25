package org.polyconf.spark.datasource

import com.google.cloud.pubsub.v1.{Publisher, SubscriptionAdminClient}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.protobuf.ByteString
import com.google.pubsub.v1._
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.polyconf.cli.stream.WriteMode
import org.polyconf.util.PolyUtil

import scala.jdk.CollectionConverters._
import scala.util.Using
import java.util.concurrent.TimeUnit

class SparkPubsubIO extends SparkDataIO {
  override def name: String = "pubsub"

  override def help: String =
    """
      |PubSub datasource: reads/writes Google Cloud PubSub
      |Reader: path format projectId:topicId:subscriptionId
      |  Options: maxMessages - max messages to pull from subscription
      |Writer: path format projectId:topicId
      |  Note: mode is accepted but ignored (PubSub topics are append-only)
      |  Options: ack - if "true", acknowledges source messages after write
      |removeStorage: path format projectId:topicId:subscriptionId, deletes subscription
      |""".stripMargin

  override def reader(
      spark: SparkSession,
      path: String,
      options: Map[String, String] = Map.empty
  ): DataFrame = {
    val ps = PubsubName.parse(path)
    ps.readMessages(spark, options.get("maxMessages").map(_.toInt))
  }

  override def writer(
      df: DataFrame,
      path: String,
      mode: WriteMode = WriteMode.Overwrite,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val ps = PubsubName.parse(path)
    ps.writeMessages(df)
    if (options.get("ack").contains("true") && df.columns.contains("ackId"))
      ps.acknowledgeMessages(df)
  }

  override def removeStorage(
      storageName: String,
      sparkSession: SparkSession,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val ps = PubsubName.parse(storageName)
    ps.deleteSubscription()
  }

  def ack(df: DataFrame, pubsubPath: String): Unit = {
    val ps = PubsubName.parse(pubsubPath)
    ps.acknowledgeMessages(df)
  }
}

private case class PubsubName(projectId: String, topicId: String, subscriptionId: Option[String]) {

  private def fullTopicName: String = ProjectTopicName.of(projectId, topicId).toString

  private def fullSubscriptionName: String = {
    val sid = subscriptionId.getOrElse(
      throw new IllegalArgumentException("subscriptionId is required but was not provided"))
    ProjectSubscriptionName.of(projectId, sid).toString
  }

  private val log = LogManager.getLogger(classOf[SparkPubsubIO])

  def readMessages(spark: SparkSession, maxMessages: Option[Int] = None): DataFrame = {
    val settings = SubscriberStubSettings.newBuilder().build()
    Using.resource(GrpcSubscriberStub.create(settings)) { subscriber =>
      val builder = PullRequest.newBuilder().setSubscription(fullSubscriptionName)
      maxMessages.foreach(m => builder.setMaxMessages(m))
      val response = subscriber.pullCallable().call(builder.build())
      val messages = response.getReceivedMessagesList.asScala
      val rows = messages.map(msg => Row(msg.getMessage.getData.toStringUtf8, msg.getAckId))
      val schema = StructType(Seq(
        StructField("data", StringType, nullable = false),
        StructField("ackId", StringType, nullable = false)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)
    }
  }

  def writeMessages(df: DataFrame): Unit = {
    df.toJSON.foreachPartition { iter: Iterator[String] =>
      PolyUtil.withData(Publisher.newBuilder(fullTopicName).build())(
        closer = p => { p.shutdown(); p.awaitTermination(30, TimeUnit.SECONDS) }
      ) { publisher =>
        iter.foreach { jsonRow =>
          val data = ByteString.copyFromUtf8(jsonRow)
          val message = PubsubMessage.newBuilder().setData(data).build()
          publisher.publish(message).get(30, TimeUnit.SECONDS)
        }
      }.failed.foreach { e =>
        log.error(s"Failed to write messages: ${e.getMessage}", e)
        throw e
      }
    }
  }

  def acknowledgeMessages(df: DataFrame): Unit = {
    df.select("ackId").rdd.foreachPartition { partition =>
      val ackIds = partition.map(_.getString(0)).toList
      if (ackIds.nonEmpty) {
        PolyUtil.withResource(SubscriptionAdminClient.create()) { client =>
          val ackRequest = AcknowledgeRequest.newBuilder()
            .setSubscription(fullSubscriptionName)
            .addAllAckIds(ackIds.asJava)
            .build()
          client.acknowledge(ackRequest)
        }.failed.foreach { e =>
          log.error(s"Failed to acknowledge messages on executor: ${e.getMessage}", e)
          throw e
        }
      }
    }
    log.info(s"Acknowledged messages from $fullSubscriptionName")
  }

  def deleteSubscription(): Unit = {
    Using.resource(SubscriptionAdminClient.create()) { client =>
      client.deleteSubscription(fullSubscriptionName)
      log.info(s"Deleted subscription $fullSubscriptionName")
    }
  }
}

private object PubsubName {
  def parse(path: String): PubsubName = {
    path.split(":") match {
      case Array(projectId, topicId, subscriptionId) =>
        PubsubName(projectId, topicId, Some(subscriptionId))
      case Array(projectId, topicId) =>
        PubsubName(projectId, topicId, None)
      case Array(topicId) =>
        val projectId = sys.env.getOrElse("GOOGLE_CLOUD_PROJECT",
          throw new IllegalArgumentException(
            s"Missing projectId; set GOOGLE_CLOUD_PROJECT env or use format projectId:topicId"
          ))
        PubsubName(projectId, topicId, None)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid path '$path'. Expected format: projectId:topicId:subscriptionId"
        )
    }
  }
}
