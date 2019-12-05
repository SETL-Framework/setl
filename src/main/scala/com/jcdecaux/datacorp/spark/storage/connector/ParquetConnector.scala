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

  override val storage: Storage = Storage.PARQUET
  this.options.setStorage(storage)

  def this(spark: SparkSession, options: ConnectorConf) = this(options)

  def this(spark: SparkSession, options: Map[String, String]) = this(spark, ConnectorConf.fromMap(options))

  def this(spark: SparkSession, path: String, saveMode: SaveMode) =
    this(spark, Map[String, String](
      "path" -> path,
      "saveMode" -> saveMode.toString
    ))

  def this(spark: SparkSession, config: Config) = this(spark = spark, options = TypesafeConfigUtils.getMap(config))

  def this(spark: SparkSession, conf: Conf) = this(spark = spark, options = conf.toMap)

}
