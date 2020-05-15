package com.jcdecaux.setl.config

/**
 * Configuration parameters: <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources">
 * Spark documentation</a>
 */
class StructuredStreamingConnectorConf extends ConnectorConf {

  import StructuredStreamingConnectorConf._

  def setFormat(format: String): this.type = set(FORMAT.toLowerCase(), format)

  def getFormat: String = getWithException(FORMAT).toLowerCase()

  def setSchema(schema: String): this.type = set(SCHEMA, schema)

  def getSchema: String = getWithException(SCHEMA)

  def setOutputMode(mode: String): this.type = set(OUTPUT_MODE, mode)

  def getOutputMode: String = getWithException(OUTPUT_MODE)

  def setPath(path: String): this.type = set(PATH, path)

  def getPath: String = getWithException(PATH)

  override def getReaderConf: Map[String, String] = removePrivateConf()

  override def getWriterConf: Map[String, String] = removePrivateConf()

  private[this] def getWithException(key: String): String = {
    get(key).getOrElse(throw new IllegalArgumentException(s"Can't find $key"))
  }

  private[this] def removePrivateConf(): Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - FORMAT - SCHEMA - OUTPUT_MODE
  }
}

object StructuredStreamingConnectorConf {
  def fromMap(options: Map[String, String]): StructuredStreamingConnectorConf =
    new StructuredStreamingConnectorConf().set(options)

  val FORMAT: String = "format"
  val SCHEMA: String = "schema"
  val OUTPUT_MODE: String = "outputMode"
  val PATH: String = "path"

}
