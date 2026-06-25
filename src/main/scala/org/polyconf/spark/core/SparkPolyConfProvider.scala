package org.polyconf.spark.core

import org.polyconf.argfmt.ParamsGeneratorBase
import org.polyconf.cli.stream.{StreamGenerator, StreamTransformer, StreamWriter, TransformerJob}
import org.polyconf.core.{PolyConf, PolyConfProvider}

class SparkPolyConfProvider extends PolyConfProvider {
  override def getConcreteClasses: java.util.Collection[Class[_ <: PolyConf]] =
    SparkPolyConfProvider.concrete
  override def getBaseClasses: java.util.Collection[Class[_ <: PolyConf]] =
    SparkPolyConfProvider.bases
}

object SparkPolyConfProvider {
  private val (concrete, bases) = PolyConfProvider.registerAllChildForBases(
    classOf[TransformerJob[_]],
    classOf[StreamGenerator[_]],
    classOf[StreamTransformer[_]],
    classOf[StreamWriter[_]],
    classOf[ParamsGeneratorBase]
  )
}
