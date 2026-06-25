package org.polyconf.spark.stream

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.polyconf.cli.stream.{StreamWriter, FileSource, StreamDataEnvelope, WriteMode}
import org.polyconf.spark.core.SparkLogRelay
import org.polyconf.spark.datasource.SparkDataIO
import org.polyconf.util.PolyUtil

import scala.util.Try

class SparkWriterImpl extends StreamWriter[DFData]
    with FileSource {

  val mode: WriteMode = WriteMode.Overwrite
  val partition: Int = -1

  @transient
  private lazy val log = LogManager.getLogger(getClass)

  override def write(input: StreamDataEnvelope[DFData]): Try[Unit] = Try {
    input.data.foreach { frameData =>
      val dfOut = repartitionIfNeeded(frameData.df)
      val allOptions = options ++ frameData.options.map { case (k, v) => k -> v.toString }
      val src = Try(SparkDataIO.get(format)).toOption
      src match {
        case Some(ds) =>
          ds.writer(dfOut, path, mode, allOptions)
        case None =>
          log.warn(s"Format '$format' not registered as a SparkDataIO, falling back to generic Spark DataSource API")
          var dfw = dfOut.write.mode(mode.toString).format(format)
          for ((k, v) <- allOptions) dfw = dfw.option(k, v)
          dfw.save(path)
      }
    }
  }

  private def repartitionIfNeeded(df: DataFrame): DataFrame = {
    if (partition > 0) {
      val current = df.rdd.getNumPartitions
      if (current != partition) {
        log.info(s"Repartitioning from $current to $partition partitions")
        df.repartition(partition)
      } else {
        log.info(s"DataFrame already has $partition partitions, no repartition needed")
        df
      }
    } else {
      log.info(s"partition=$partition (<=0), keeping current partitioning")
      df
    }
  }

  override def help: String =
    s"""
       |Spark DataFrame writer.
       |Writes DataFrames using Spark DataSource API.
       |path: output path
       |format: one of ${SparkDataIO.available.mkString(", ")} or any Spark format
       |mode: overwrite, append, error, ignore
       |partition: target partitions (<=0 = keep current, -1 = default)
       |options: Spark write options (format-specific)
       |""".stripMargin
}

class SparkStatsWriter extends StreamWriter[DFData] {
  import SparkStatsWriter.log

  val showDebugRowsNumber: Int = 10
  val showRows: Boolean = false
  val jsonFormat: Boolean = false
  val showWorkerRows: Int = 0

  override def write(input: StreamDataEnvelope[DFData]): Try[Unit] = Try {
    input.data.foreach { frameData =>
      PolyUtil.withData(frameData.df.cache())(_.unpersist()) { df =>
        val count = df.count()
        val partitions = df.rdd.getNumPartitions

        if (showRows) {
          val n = math.min(showDebugRowsNumber, if (count <= Int.MaxValue) count.toInt else Int.MaxValue)
          if (n > 0) {
            log.info(s"Data sample ($n/$count)")
            if (jsonFormat) {
              df.limit(n).toJSON.collect().foreach { json =>
                log.info(s"  $json")
              }
            } else {
              val rows = df.limit(n).collect()
              rows.foreach { row =>
                log.info(s"  ${row.mkString(", ")}")
              }
            }
          }
        }

        if (showWorkerRows > 0) {
          val rowsPerPart = math.max(1, showWorkerRows / partitions)
          log.info(s"Worker rows: requesting $showWorkerRows rows ($rowsPerPart per partition)")
          SparkLogRelay.getLogAccumulator match {
            case Some(acc) =>
              df.foreachPartition { iter: Iterator[org.apache.spark.sql.Row] =>
                val rows = iter.take(rowsPerPart).map(r => r.mkString(", ")).toSeq
                val workerLog = SparkLogRelay.getLogger("app")
                rows.foreach { workerLog.warn }
                SparkLogRelay.flushWorkerLogs(acc)
                ()
              }
            case None =>
              log.warn("Log accumulator not available, skipping worker logging")
          }
        }

        log.info(
          s"Path ${input.inputPath} Partitions $partitions count $count " +
            s"rowsPerPart ${count / math.max(1, partitions)}"
        )
      }.get
    }
  }

  override def help: String =
    """
      |Spark DataFrame statistics writer.
      |Prints DataFrame statistics and sample rows to the log.
      |showDebugRowsNumber: number of sample rows to print on driver (default: 10)
      |showRows: enable sample row printing (default: false)
      |jsonFormat: print sample rows as JSON (default: false)
      |showWorkerRows: distribute this many rows across partitions for per-worker logging (default: 0, disabled)
      |""".stripMargin
}

object SparkStatsWriter {
  private val log = LogManager.getLogger(classOf[SparkStatsWriter])
}
