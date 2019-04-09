package com.jcdecaux.datacorp.spark.storage.parquet

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CSVConnector
  */
trait ParquetConnector {

  private[this] final val logger: Logger = Logger.getLogger(this.getClass)

  val spark: SparkSession
  val path: String
  val table: String

  protected def readParquet(): DataFrame = {
    logger.debug(s"Reading csv file from $path")
    this.spark.read.parquet(path)
  }

  /**
    *
    * @param df
    */
  protected def writeParquet(df: DataFrame, saveMode: SaveMode): Unit = {
    logger.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .option("path", path)
      .saveAsTable(table)
  }
}
