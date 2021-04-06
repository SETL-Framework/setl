package io.github.setl.config

import io.github.setl.annotation.InterfaceStability
import io.github.setl.enums.Storage
import io.github.setl.exception.ConfException
import io.github.setl.annotation.InterfaceStability.Evolving
import org.apache.spark.sql.SaveMode

@Evolving
class FileConnectorConf extends ConnectorConf {

  private[this] val defaultEncoding: String = "UTF-8"
  private[this] val defaultSaveMode: String = SaveMode.ErrorIfExists.toString
  private[this] val defaultNative: String = "false"

  def setStorage(storage: String): this.type = set("storage", storage.toUpperCase)

  def setStorage(storage: Storage): this.type = setStorage(storage.toString)

  def setEncoding(encoding: String): this.type = set("encoding", encoding)

  def setSaveMode(saveMode: String): this.type = set("saveMode", saveMode)

  def setSaveMode(saveMode: SaveMode): this.type = set("saveMode", saveMode.toString)

  def setPath(path: String): this.type = set("path", path)

  def setNative(native: String): this.type = set("native", native)

  def setS3CredentialsProvider(value: String): this.type = set("fs.s3a.aws.credentials.provider", value)

  def setS3AccessKey(value: String): this.type = set("fs.s3a.access.key", value)

  def setS3SecretKey(value: String): this.type = set("fs.s3a.secret.key", value)

  def setS3SessionToken(value: String): this.type = set("fs.s3a.session.token", value)

  def getEncoding: String = get("encoding", defaultEncoding)

  def getSaveMode: SaveMode = SaveMode.valueOf(get("saveMode", defaultSaveMode))

  def getNative: String = get("native", defaultNative)

  def getStorage: Storage = get("storage") match {
    case Some(storage) => Storage.valueOf(storage)
    case _ => throw new ConfException("The value of storage is not set")
  }

  /**
   * Get path of file to be loaded.
   * It could be a file path or a directory (for CSV and Parquet).
   * In the case of a directory, the correctness of Spark partition structure should be guaranteed by user.
   */
  def getPath: String = get("path") match {
    case Some(path) => path
    case _ => throw new ConfException("The value of path is not set")
  }

  def getSchema: Option[String] = get("schema")

  def getS3CredentialsProvider: Option[String] = get("fs.s3a.aws.credentials.provider")

  def getS3AccessKey: Option[String] = get("fs.s3a.access.key")

  def getS3SecretKey: Option[String] = get("fs.s3a.secret.key")

  def getS3SessionToken: Option[String] = get("fs.s3a.session.token")

  def getFilenamePattern: Option[String] = get("filenamePattern")

  /**
   * Extra options that will be passed into DataFrameReader. Keys like "path" should be removed
   */
  override def getReaderConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap -
      "path" -
      "filenamePattern" -
      "saveMode" -
      "schema" -
      "fs.s3a.aws.credentials.provider" -
      "fs.s3a.access.key" -
      "fs.s3a.secret.key" -
      "fs.s3a.session.token"
  }

  /**
   * Extra options that will be passed into DataFrameWriter. Keys like "path" should be removed
   */
  override def getWriterConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap -
      "path" -
      "filenamePattern" -
      "fs.s3a.aws.credentials.provider" -
      "fs.s3a.access.key" -
      "fs.s3a.secret.key" -
      "fs.s3a.session.token"
  }

}

object FileConnectorConf {
  def fromMap(options: Map[String, String]): FileConnectorConf = new FileConnectorConf().set(options)
}
