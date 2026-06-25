package org.polyconf.spark

object TestSparkMaster {
  val url: String = sys.props.getOrElse("spark.master", sys.env.getOrElse("SPARK_MASTER", "local[1]"))
  val isStandalone: Boolean = url.startsWith("spark://")

  def config: Map[String, String] = {
    if (isStandalone) {
      Map("spark.executor.extraClassPath" -> sys.props("java.class.path"))
    } else {
      Map.empty
    }
  }
}
