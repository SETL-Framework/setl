package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.{Conf, ConnectorConf}
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._

/**
 * ParquetConnector contains functionality for transforming [[DataFrame]] into parquet files
 */
@InterfaceStability.Evolving
class ParquetConnector(override val options: ConnectorConf) extends FileConnector(options) {

  def this(options: Map[String, String]) = this(ConnectorConf.fromMap(options))

  def this(path: String, saveMode: SaveMode) = this(Map("path" -> path, "saveMode" -> saveMode.toString))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, options: ConnectorConf) = this(options)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, options: Map[String, String]) = this(ConnectorConf.fromMap(options))

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, path: String, saveMode: SaveMode) = this(path, saveMode)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, config: Config) = this(TypesafeConfigUtils.getMap(config))

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, conf: Conf) = this(conf.toMap)

  override val storage: Storage = Storage.PARQUET

  this.options.setStorage(storage)
}
