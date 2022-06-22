package io.github.setl.config

import io.github.setl.exception.ConfException
import org.apache.spark.sql.SaveMode

class HudiConnectorConf extends ConnectorConf {

  import HudiConnectorConf._

  override def getReaderConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - PATH
  }

  override def getWriterConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - SAVEMODE - PATH
  }

  def setPath(path: String): this.type = set("path", path)

  def setSaveMode(saveMode: String): this.type = set("saveMode", saveMode)

  def setSaveMode(saveMode: SaveMode): this.type = set("saveMode", saveMode.toString)

  def getPath: String = get("path") match {
    case Some(path) => path
    case _ => throw new ConfException("The value of path is not set")
  }

  def getSaveMode: SaveMode = SaveMode.valueOf(get("saveMode", SaveMode.Append.toString))

}

object HudiConnectorConf {
  def fromMap(options: Map[String, String]): HudiConnectorConf = new HudiConnectorConf().set(options)

  val SAVEMODE: String = "saveMode"
  val PATH: String = "path"
}
