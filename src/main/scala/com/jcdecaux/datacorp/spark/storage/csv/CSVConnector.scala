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
    *
    * @param df
    */
  protected def writeCSV(df: DataFrame, saveMode: SaveMode): Unit = {
    logger.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .csv(path)
  }
}
