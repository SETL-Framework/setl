package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.config.Conf
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
 *
 * @param region     region of AWS
 * @param table      table name
 * @param saveMode   save mode
 * @param throughput the desired read/write throughput to use
 */
@InterfaceStability.Evolving
class DynamoDBConnector(val region: String, // "eu-west-1"
                        val table: String,
                        val saveMode: SaveMode,
                        val throughput: String
                       ) extends DBConnector {

  def this(config: Config) = this(
    region = TypesafeConfigUtils.getAs[String](config, "region").get,
    table = TypesafeConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").getOrElse(SaveMode.ErrorIfExists.toString)),
    throughput = TypesafeConfigUtils.getAs[String](config, "throughput").getOrElse("10000")
  )

  def this(conf: Conf) = this(
    region = conf.get("region").get,
    table = conf.get("table").get,
    saveMode = SaveMode.valueOf(conf.get("saveMode", SaveMode.ErrorIfExists.toString)),
    throughput = conf.get("throughput", "10000")
  )

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

  override val reader: DataFrameReader = {
    log.debug(s"DynamoDB connector read throughput $throughput")
    spark.read
      .option("region", region)
      .option("throughput", throughput)
      .format("com.audienceproject.spark.dynamodb.datasource")
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .mode(saveMode)
      .option("region", region)
      .option("throughput", throughput)
      .format("com.audienceproject.spark.dynamodb.datasource")
  }

  override val storage: Storage = Storage.DYNAMODB

  private[this] def writeDynamoDB(df: DataFrame, tableName: String): Unit = {
    writer(df).option("tableName", tableName).save()
  }

  override def read(): DataFrame = {
    log.debug(s"Reading DynamoDB table $table in $region")
    reader.option("tableName", table).load()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Suffix will be ignore in DynamoDBConnector")
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
    writeDynamoDB(t, table)
  }
}