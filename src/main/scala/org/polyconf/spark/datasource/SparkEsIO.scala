package org.polyconf.spark.datasource

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.polyconf.cli.stream.WriteMode
import org.polyconf.util.PolyUtil

import java.net.{HttpURLConnection, URI}

class SparkEsIO extends SparkDataIO {
  override def name: String = "elasticsearch"

  private val log = LogManager.getLogger(getClass)

  override def help: String =
    """
      |Elasticsearch datasource: reads/writes Elasticsearch indices via ES Spark connector.
      |Reader: path format indexName
      |  Options: es.nodes, es.port, es.nodes.wan.only, es.apiKey,
      |           es.net.http.auth.user, es.net.http.auth.pass
      |Writer: path format indexName
      |  Options: same as reader, plus standard ES connector options
      |removeStorage: HTTP DELETE to delete the index
      |""".stripMargin

  private val esPromotedOptions = Set(
    "es.nodes", "es.port", "es.nodes.wan.only",
    "es.apiKey",
    "es.net.http.auth.user", "es.net.http.auth.pass"
  )

  override def reader(
      spark: SparkSession,
      path: String,
      options: Map[String, String] = Map.empty
  ): DataFrame = {
    var dfr = spark.read.format("es").option("resource", path)
    for ((k, v) <- options) dfr = dfr.option(k, v)
    dfr.load()
  }

  override def writer(
      df: DataFrame,
      path: String,
      mode: WriteMode = WriteMode.Overwrite,
      options: Map[String, String] = Map.empty
  ): Unit = {
    if (mode == WriteMode.Overwrite)
      removeStorage(path, df.sparkSession, options)

    var dfw = df.write.format("es").mode(mode.toString).option("resource", path)
    for ((k, v) <- options) dfw = dfw.option(k, v)
    dfw.save()
  }

  override def removeStorage(
      storageName: String,
      sparkSession: SparkSession,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val esConfig = EsConnectionConfig.fromOptions(options)
    val url = esConfig.indexUrl(storageName)
    PolyUtil.withData(url.openConnection().asInstanceOf[HttpURLConnection])(_.disconnect()) { conn =>
      conn.setRequestMethod("DELETE")
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(10000)
      for ((k, v) <- esConfig.headers) conn.setRequestProperty(k, v)
      conn.connect()
      val code = conn.getResponseCode
      if (code != 200 && code != 404)
        throw new RuntimeException(
          s"Failed to delete ES index $storageName: HTTP $code"
        )
    }.failed.foreach { e =>
      log.error(s"Failed to delete ES index $storageName: ${e.getMessage}", e)
      throw e
    }
  }

  private case class EsConnectionConfig(
      nodes: String,
      port: String,
      protocol: String,
      headers: Map[String, String]
  ) {
    def indexUrl(index: String): java.net.URL =
      new URI(s"$protocol://$nodes:$port/$index").toURL
  }

  private object EsConnectionConfig {
    def fromOptions(options: Map[String, String]): EsConnectionConfig = {
      val nodes = options.getOrElse("es.nodes", "localhost")
      val port = options.getOrElse("es.port", "9200")
      val apiKey = options.get("es.apiKey")
      val user = options.get("es.net.http.auth.user")
      val pass = options.get("es.net.http.auth.pass")
      val protocol = options.getOrElse("es.nodes.wan.only", "false") match {
        case "true" => "https"
        case _      => "http"
      }

      val authHeaders: Map[String, String] =
        if (apiKey.exists(_.nonEmpty))
          Map("Authorization" -> s"ApiKey ${apiKey.get}")
        else if (user.exists(_.nonEmpty) && pass.exists(_.nonEmpty))
          Map("Authorization" -> ("Basic " + java.util.Base64.getEncoder
            .encodeToString(s"${user.get}:${pass.get}".getBytes("UTF-8"))))
        else
          Map.empty

      EsConnectionConfig(nodes, port, protocol, authHeaders)
    }
  }
}
