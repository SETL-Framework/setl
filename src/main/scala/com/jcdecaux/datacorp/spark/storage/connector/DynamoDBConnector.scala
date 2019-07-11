package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
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
  * @param spark    spark session
  * @param region   region of AWS
  * @param table    table name
  * @param saveMode save mode
  */
@InterfaceStability.Evolving
class DynamoDBConnector(val spark: SparkSession,
                        val region: String, // "eu-west-1"
                        val table: String,
                        val saveMode: SaveMode) extends DBConnector with Logging {

  import com.audienceproject.spark.dynamodb.implicits._

  override var reader: DataFrameReader = spark.read.option("region", region)
  override var writer: DataFrameWriter[Row] = _

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    region = TypesafeConfigUtils.getAs[String](config, "region").get,
    table = TypesafeConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").get)
  )

  def this(spark: SparkSession, conf: Conf) = this(
    spark = spark,
    region = conf.get("region").get,
    table = conf.get("table").get,
    saveMode = SaveMode.valueOf(conf.get("region").get)
  )

  @inline private[this] def initWriter(df: DataFrame): Unit = {
    if (df.hashCode() != lastWriteHashCode) {
      writer = df.write
        .mode(saveMode)
        .option("region", region)

      lastWriteHashCode = df.hashCode()
    }
  }

  override val storage: Storage = Storage.DYNAMODB

  private[this] def writeDynamoDB(df: DataFrame, tableName: String): Unit = {
    initWriter(df)
    writer.dynamodb(tableName)
  }

  override def read(): DataFrame = {
    log.debug(s"Reading DynamoDB table $table in $region")
    reader.dynamodb(table)
  }

  override def write(t: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) => writeDynamoDB(t, s"$table/$s")
      case _ => writeDynamoDB(t, table)
    }
  }

  override def create(t: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Create is not supported in DynamoDBConnector")
  }

  override def delete(query: String): Unit = {
    log.warn("Delete is not supported in DynamoDBConnector")
  }
}