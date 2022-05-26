package io.github.setl.storage.connector

import com.typesafe.config.Config
import io.github.setl.config.{Conf, HudiConnectorConf}
import io.github.setl.enums.Storage
import io.github.setl.internal.HasReaderWriter
import io.github.setl.util.TypesafeConfigUtils
import org.apache.spark.sql._

class HudiConnector(val options: HudiConnectorConf) extends Connector with HasReaderWriter {
  override val storage: Storage = Storage.HUDI

  def this(options: Map[String, String]) = this(HudiConnectorConf.fromMap(options))

  def this(path: String, saveMode: SaveMode) = this(Map("path" -> path, "saveMode" -> saveMode.toString))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  override val reader: DataFrameReader = {
    spark.read
      .format("hudi")
      .options(options.getReaderConf)
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .format("hudi")
      .mode(options.getSaveMode)
      .options(options.getWriterConf)
  }

  /**
   * Read data from the data source
   *
   * @return a [[DataFrame]]
   */
  @throws[java.io.FileNotFoundException](s"${options.getPath} doesn't exist")
  @throws[org.apache.spark.sql.AnalysisException](s"${options.getPath} doesn't exist")
  override def read(): DataFrame = {
    logDebug(s"Reading ${storage.toString} file in: '${options.getPath}'")
    this.setJobDescription(s"Read file(s) from '${options.getPath}'")
    reader.load(options.getPath)
  }

  /**
   * Write a [[DataFrame]] into the data storage
   *
   * @param t      a [[DataFrame]] to be saved
   * @param suffix for data connectors that support suffix (e.g. [[FileConnector]],
   *               add the given suffix to the save path
   */
  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    if (suffix.isDefined) logWarning("Suffix is not supported in HudiConnector")
    write(t)
  }

  /**
   * Write a [[DataFrame]] into the data storage
   *
   * @param t a [[DataFrame]] to be saved
   */
  override def write(t: DataFrame): Unit = {
    this.setJobDescription(s"Write file to ${options.getPath}")
    writer(t).save(options.getPath)
  }
}
