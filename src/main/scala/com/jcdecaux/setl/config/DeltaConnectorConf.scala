package com.jcdecaux.setl.config

import com.jcdecaux.setl.exception.ConfException
import org.apache.spark.sql.SaveMode

class DeltaConnectorConf extends ConnectorConf {

  import DeltaConnectorConf._

  def setPath(path: String): this.type = set("path", path)

  def setSaveMode(saveMode: String): this.type = set("saveMode", saveMode)

  def setSaveMode(saveMode: SaveMode): this.type = set("saveMode", saveMode.toString)

  def getPath: String = get("path") match {
    case Some(path) => path
    case _ => throw new ConfException("The value of path is not set")
  }

  def getSaveMode: SaveMode = SaveMode.valueOf(get("saveMode", SaveMode.Append.toString))

  override def getReaderConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - PATH
  }

  override def getWriterConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - SAVEMODE - PATH
  }
}

object DeltaConnectorConf {

  def fromMap(options: Map[String, String]): DeltaConnectorConf = new DeltaConnectorConf().set(options)

  val SAVEMODE: String = "saveMode"
  val PATH: String = "path"
}
