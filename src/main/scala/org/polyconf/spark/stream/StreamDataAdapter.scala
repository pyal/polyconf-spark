package org.polyconf.spark.stream

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.polyconf.cli.stream.{StreamGenerator, StreamData, StreamTransformer, StreamWriter, SuccessRecorderBase, StreamDataEnvelope}

import org.apache.logging.log4j.LogManager
import scala.jdk.CollectionConverters._
import scala.util.Try

object StreamDataAdapter {
  def toDataFrame(spark: SparkSession, packet: StreamData): DataFrame = {
    if (packet.data.isEmpty) {
      val emptySchema = StructType(Seq(StructField("_empty", BooleanType, nullable = true)))
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)
    }
    val schema = inferSchema(packet.data.head)
    val rows = packet.data.map(rowToRow(schema))
    spark.createDataFrame(rows.asJava, schema)
  }

  private val log = LogManager.getLogger(getClass)

  def fromDataFrame(df: DataFrame, truncationLength: Int = 1000000): StreamData = {
    val columns = df.columns
    val fieldIndex = columns.zipWithIndex.toMap
    val rows = df.limit(truncationLength + 1).collect().map { row =>
      columns.map(c => c -> row.get(fieldIndex(c))).toMap
    }
    if (rows.length > truncationLength)
      log.warn(s"DataFrame has more than $truncationLength rows, truncating to $truncationLength for StreamData conversion")
    StreamData(rows.take(truncationLength).toSeq)
  }

  private def inferSchema(row: Map[String, Any]): StructType = {
    StructType(row.map { case (name, value) =>
      StructField(name, sparkType(value), nullable = true)
    }.toSeq)
  }

  private def sparkType(value: Any): DataType = value match {
    case _: String       => StringType
    case _: Int          => IntegerType
    case _: Long         => LongType
    case _: Double       => DoubleType
    case _: Float        => FloatType
    case _: Boolean      => BooleanType
    case _: BigInt       => DecimalType(38, 0)
    case _: BigDecimal   => DecimalType.SYSTEM_DEFAULT
    case _: Seq[_]       => ArrayType(StringType)
    case _: Map[_, _]    => MapType(StringType, StringType)
    case _: java.sql.Date  => DateType
    case _: java.sql.Timestamp => TimestampType
    case null            => NullType
    case _               => StringType
  }

  private def rowToRow(schema: StructType)(map: Map[String, Any]): Row = {
    val values = schema.fields.map { field =>
      map.getOrElse(field.name, null)
    }
    Row.fromSeq(values.toIndexedSeq)
  }
}

final case class StreamDataGenerator(
    delegate: StreamGenerator[StreamData]
) extends StreamGenerator[DFData] {

  @transient
  private lazy val spark = SparkSession.builder().getOrCreate()

  override def readIterator: Iterator[StreamDataEnvelope[DFData]] =
    readIterator(None)

  override def readIterator(successRecorderOpt: Option[SuccessRecorderBase]): Iterator[StreamDataEnvelope[DFData]] =
    delegate.readIterator(successRecorderOpt).map { td =>
      td.data match {
        case scala.util.Failure(e) => StreamDataEnvelope(scala.util.Failure(e), td.inputPath)
        case scala.util.Success(packet) =>
          val df = StreamDataAdapter.toDataFrame(spark, packet)
          StreamDataEnvelope(scala.util.Success(DFData(df)), td.inputPath)
      }
    }
}

final case class StreamDataTransformer(
    delegate: StreamTransformer[StreamData],
    truncationLength: Int = 1000000
) extends StreamTransformer[DFData] {

  @transient
  private lazy val spark = SparkSession.builder().getOrCreate()

  override def transform(input: StreamDataEnvelope[DFData]): Iterator[StreamDataEnvelope[DFData]] = {
    input.data match {
      case scala.util.Failure(e) => Iterator(StreamDataEnvelope(scala.util.Failure(e), input.inputPath))
      case scala.util.Success(frameData) =>
        val packet = StreamDataAdapter.fromDataFrame(frameData.df, truncationLength)
        val td = StreamDataEnvelope(scala.util.Success(packet), input.inputPath)
        delegate.transform(td).map { result =>
          result.data match {
            case scala.util.Failure(e) => StreamDataEnvelope(scala.util.Failure(e), result.inputPath)
            case scala.util.Success(p) =>
              val df = StreamDataAdapter.toDataFrame(spark, p)
              StreamDataEnvelope(scala.util.Success(DFData(df, frameData.options)), result.inputPath)
          }
        }
    }
  }
}

final case class StreamDataWriter(
    delegate: StreamWriter[StreamData],
    truncationLength: Int = 1000000
) extends StreamWriter[DFData] {

  @transient
  private lazy val spark = SparkSession.builder().getOrCreate()

  override def write(input: StreamDataEnvelope[DFData]): Try[Unit] = {
    input.data.flatMap { frameData =>
      val packet = StreamDataAdapter.fromDataFrame(frameData.df, truncationLength)
      delegate.write(StreamDataEnvelope(scala.util.Success(packet), input.inputPath))
    }
  }
}
