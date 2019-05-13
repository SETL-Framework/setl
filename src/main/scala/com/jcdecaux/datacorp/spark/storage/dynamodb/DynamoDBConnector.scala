package com.jcdecaux.datacorp.spark.storage.dynamodb

import com.jcdecaux.datacorp.spark.storage.util.DynamoDBUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * DynamoDBConnector
  */
trait DynamoDBConnector {

  private[this] final val logger: Logger = Logger.getLogger(this.getClass)

  val spark: SparkSession
  val region: String = "eu-west-1"
  val table: String

  /**
    * readDynamoDB
    *
    * @return
    */
  protected def readDynamoDB(): DataFrame = {
    logger.debug(s"Reading DynamoDB table $table")
    DynamoDBUtils.read(spark, table, region)
  }

  /**
    * Write a DataFrame into the default table with the given save mode
    *
    * @param df
    * @param saveMode
    */
  protected def writeDynamoDB(df: DataFrame, saveMode: SaveMode): Unit = {
    this.writeDynamoDB(df, this.table, region, saveMode)
  }

  /**
    * Write a DataFrame into the given table with the given save mode
    *
    * @param df
    * @param table
    * @param region
    * @param saveMode
    */
  protected def writeDynamoDB(df: DataFrame, table: String, region: String, saveMode: SaveMode): Unit = {
    logger.debug(s"Write DataFrame to DynamoDB table $table")
    DynamoDBUtils.write(df, table, region, saveMode)
  }
}
