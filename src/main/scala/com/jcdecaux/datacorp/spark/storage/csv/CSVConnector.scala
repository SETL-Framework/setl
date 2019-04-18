package com.jcdecaux.datacorp.spark.storage.csv

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CSVConnector
  */
trait CSVConnector {

  private[this] final val logger: Logger = Logger.getLogger(this.getClass)

  val spark: SparkSession
  val path: String
  val inferSchema: String = "true"
  val delimiter: String = ";"
  val header: String = "true"

  protected def readCSV(): DataFrame = {
    logger.debug(s"Reading csv file from $path")
    this.spark.read
      .option("header", this.header)
      .option("inferSchema", this.inferSchema)
      .option("delimiter", this.delimiter)
      .csv(path)
  }

  /**
    * Write a [[DataFrame]] into the default path with the given save mode
    */
  protected def writeCSV(df: DataFrame, saveMode: SaveMode): Unit = {
    this.writeCSV(df, this.path, saveMode)
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[this] def writeCSV(df: DataFrame, path: String, saveMode: SaveMode): Unit = {
    logger.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .option("header", this.header)
      .option("delimiter", this.delimiter)
      .csv(path)
  }
}
