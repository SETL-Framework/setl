package com.jcdecaux.datacorp.spark.storage.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * DynamoDBUtils
  */
object DynamoDBUtils {

  import com.audienceproject.spark.dynamodb.implicits._

  /**
    *
    * @param spark
    * @param table
    * @param region
    * @return
    */
  def read(spark: SparkSession, table: String, region: String = "eu-west-1"): DataFrame = {
    spark.read
      .option("region", region)
      .dynamodb(table)
  }

  /**
    *
    * @param data
    * @param table
    * @param region
    * @param saveMode
    */
  def write(data: DataFrame, table: String, region: String = "eu-west-1", saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    data.write
      .mode(saveMode)
      .option("region", region)
      .dynamodb(table)
  }
}
