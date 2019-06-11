package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * ParquetConnector contains functionality for transforming [[DataFrame]] into parquet files
  */
class ParquetConnector(val spark: SparkSession,
                       val path: String,
                       val table: String,
                       val saveMode: SaveMode) extends Connector with Logging {

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path = ConfigUtils.getAs[String](config, "path").get,
    table = ConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(ConfigUtils.getAs[String](config, "saveMode").get)
  )

  /**
    * Read a [[DataFrame]] from a parquet file with the path defined during the instantiation
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from $path")
    this.spark.read.parquet(path)
  }

  /**
    * Write a [[DataFrame]] into parquet file
    *
    * @param df dataframe to be written
    */
  override def write(df: DataFrame): Unit = {
    log.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .option("path", path)
      .saveAsTable(table)
  }
}
