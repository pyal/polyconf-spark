package org.polyconf.spark.core

import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.{Logger => Log4jCoreLogger, LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.core.config.LoggerConfig
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.util.CollectionAccumulator
import org.polyconf.util.PolyLog

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

object SparkLogRelay {
  private val log = LogManager.getLogger(getClass)
  private var logAcc: Option[CollectionAccumulator[String]] = None
  private val execLoggers = mutable.Map.empty[String, CaptureAppender]

  private def resolveLogRules: String = {
    val result = Try(
      Option(SparkEnv.get).map(_.conf.get("spark.polyconf.logRules", PolyLog.currentLogRules)).getOrElse(PolyLog.currentLogRules)
    )
    result.failed.foreach { e =>
      log.error(s"Failed to resolve log rules: ${e.getMessage}", e)
    }
    result.getOrElse(PolyLog.currentLogRules)
  }

  def init(sc: SparkContext): Unit = {
    if (logAcc.isEmpty) {
      val acc = new CollectionAccumulator[String]
      sc.register(acc, "polyconf.logs")
      logAcc = Some(acc)
    }
  }

  private[spark] def getLogAccumulator: Option[CollectionAccumulator[String]] = logAcc

  def getLogger(name: String): Logger = execLoggers.synchronized {
    execLoggers.get(name) match {
      case Some(buf) => buf.logger
      case None =>
        val buf = new CaptureAppender(name, resolveLogRules)
        execLoggers.put(name, buf)
        buf.logger
    }
  }

  /** Flush captured logs from this JVM into the given accumulator.
    * Use this on executors where logAcc singleton field is not available.
    * Returns the number of log lines flushed.
    */
  def flushWorkerLogs(acc: CollectionAccumulator[String]): Int = execLoggers.synchronized {
    execLoggers.values.map { buf =>
      val lines = buf.drain()
      lines.foreach(acc.add)
      lines.size
    }.sum
  }

  /** Flush captured logs from this JVM into the stored accumulator.
    * Use this on the driver where init() has been called.
    */
  def flushWorkerLogs(): Int = {
    logAcc.map(flushWorkerLogs).getOrElse(0)
  }

  def printMasterLogs(): Unit = {
    flushWorkerLogs()
    logAcc.foreach { acc =>
      val logs = acc.value.asScala.toSeq
      if (logs.nonEmpty) {
        println(s"======== printMasterLogs(${logs.size} lines) =========")
        logs.foreach(println)
        println("======== end printMasterLogs =========")
        acc.reset()
      }
    }
  }

  def getMasterLogs: Seq[String] =
    logAcc.map(_.value.asScala.toSeq).getOrElse(Seq.empty)

  def clear(): Unit = execLoggers.synchronized {
    logAcc.foreach(_.reset())
    execLoggers.clear()
  }
}

private class CaptureAppender(name: String, logRules: String) {
  private val logLevel = PolyLog.getLevel(name, logRules)
  private val buf = new StringBuffer
  private val layout = PatternLayout.newBuilder()
    .withPattern("%d{HH:mm:ss.SSS} [%t] %-5p (%F:%L) %M - %m%n")
    .build()
  @annotation.nowarn("cat=deprecation")
  private val appender = new AbstractAppender(s"capture-$name", null, layout) {
    override def append(event: LogEvent): Unit = {
      buf.append(layout.toSerializable(event))
    }
  }
  appender.start()

  Configurator.setLevel(name, logLevel)

  private val coreLogger = LogManager.getLogger(name).asInstanceOf[Log4jCoreLogger]

  // Use full Configuration API to add the appender
  private val ctx = LoggerContext.getContext(false)
  private val cfg = ctx.getConfiguration
  cfg.addAppender(appender)
  private val lc = cfg.getLoggerConfig(name)
  if (lc.getName != name) {
    val newLc = new LoggerConfig(name, logLevel, true)
    newLc.addAppender(appender, null, null)
    cfg.addLogger(name, newLc)
  } else {
    lc.addAppender(appender, null, null)
  }
  ctx.updateLoggers()

  val logger: Logger = coreLogger

  def drain(): Seq[String] = buf.synchronized {
    if (buf.length == 0) return Seq.empty
    val content = buf.toString
    buf.setLength(0)
    content.split("\n").toSeq.filter(_.nonEmpty)
  }
}
