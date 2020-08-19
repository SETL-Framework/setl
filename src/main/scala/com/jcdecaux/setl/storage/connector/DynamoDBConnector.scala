package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.config.{Conf, DynamoDBConnectorConf}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._


/**
 * DynamoDB connector.
 *
 * {{{
 *   # Configuration
 *   dynamodb {
 *     region = ""
 *     table = ""
 *     saveMode = ""
 *   }
 * }}}
 */
@InterfaceStability.Evolving
class DynamoDBConnector(val conf: DynamoDBConnectorConf) extends DBConnector {

  def this(conf: Map[String, String]) = this(new DynamoDBConnectorConf().set(conf))

  def this(region: String,
           table: String,
           saveMode: SaveMode,
           throughput: String) = {
    this(
      new DynamoDBConnectorConf().set(
        Map(
          DynamoDBConnectorConf.REGION -> region,
          DynamoDBConnectorConf.TABLE -> table,
          DynamoDBConnectorConf.Writer.UPDATE -> "true",
          DynamoDBConnectorConf.THROUGHPUT -> throughput
        )
      )
    )
  }

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession,
           region: String, // "eu-west-1"
           table: String,
           saveMode: SaveMode,
           throughput: String = "10000") = this(region, table, saveMode, throughput)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, config: Config) = this(config)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, conf: Conf) = this(conf)

  private[this] val sourceName: String = "com.audienceproject.spark.dynamodb.datasource"

  require(conf.getTable.nonEmpty, "DynamoDB table is not defined")
  require(conf.getRegion.nonEmpty, "DynamoDB region is not defined")

  override val reader: DataFrameReader = {
    log.debug(s"DynamoDB connector read throughput ${conf.get(DynamoDBConnectorConf.THROUGHPUT)}")
    spark.read
      .options(conf.getReaderConf)
      .format(sourceName)
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .options(conf.getWriterConf)
      .mode(conf.getSaveMode)
      .format(sourceName)
  }

  override val storage: Storage = Storage.DYNAMODB

  private[this] def writeDynamoDB(df: DataFrame, tableName: String): Unit = {
    this.setJobDescription(s"Write data to table $tableName}")
    conf.getWriterConf.foreach(log.debug)

    import com.audienceproject.spark.dynamodb.implicits._
    df.write
      .options(conf.getWriterConf)
      .mode(conf.getSaveMode)
      .dynamodb(tableName)
  }

  override def read(): DataFrame = {
    this.setJobDescription(s"Write data to table ${conf.getTable.getOrElse("unknown")}")
    log.debug(s"Reading DynamoDB table ${conf.getTable.get} in ${conf.getRegion.get}")
    conf.getReaderConf.foreach(log.debug)
    reader.option("tableName", conf.getTable.get).load()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Suffix will be ignored in DynamoDBConnector")
    write(t)
  }

  override def create(t: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Create is not supported in DynamoDBConnector")
  }

  override def delete(query: String): Unit = {
    log.warn("Delete is not supported in DynamoDBConnector")
  }

  override def create(t: DataFrame): Unit = {
    log.warn("Create is not supported in DynamoDBConnector")
  }

  override def write(t: DataFrame): Unit = {
    writeDynamoDB(t, conf.getTable.get)
  }

  override def drop(): Unit = {
    log.warn("Drop is not supported in DynamoDBConnector")
  }

}
