package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


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
class DynamoDBConnector(val spark: SparkSession,
                        val region: String, // "eu-west-1"
                        val table: String,
                        val saveMode: SaveMode) extends Connector with Logging {

  import com.audienceproject.spark.dynamodb.implicits._

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    region = ConfigUtils.getAs[String](config, "region").get,
    table = ConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(ConfigUtils.getAs[String](config, "saveMode").get)
  )

  override val storage: Storage = Storage.DYNAMODB

  override def read(): DataFrame = {
    log.debug(s"Reading DynamoDB table $table in $region")
    spark.read.option("region", region).dynamodb(table)
  }

  override def write(t: DataFrame): Unit = {
    t.write
      .mode(saveMode)
      .option("region", region)
      .dynamodb(table)
  }
}
