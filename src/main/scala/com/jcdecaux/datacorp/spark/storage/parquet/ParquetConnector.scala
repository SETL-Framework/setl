package com.jcdecaux.datacorp.spark.storage.parquet

import com.jcdecaux.datacorp.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * ParquetConnector contains functionality for transforming [[DataFrame]] into parquet files
  */
trait ParquetConnector extends Logging {

  val spark: SparkSession
  val path: String
  val table: String

  /**
    * Read a [[DataFrame]] from a parquet file with the path defined during the instantiation
    *
    * @return
    */
  protected def readParquet(): DataFrame = {
    log.debug(s"Reading Parquet file from $path")
    this.spark.read.parquet(path)
  }

  /**
    * Write a [[DataFrame]] into parquet file
    *
    * @param df dataframe
    */
  protected def writeParquet(df: DataFrame, saveMode: SaveMode): Unit = {
    log.debug(s"Write DataFrame to $path in Parquet format")
    df.write
      .mode(saveMode)
      .option("path", path)
      .saveAsTable(table)
  }
}
