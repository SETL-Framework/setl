package com.jcdecaux.datacorp.spark.storage.csv

import com.jcdecaux.datacorp.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CSVConnector contains functionality for transforming [[DataFrame]] into csv files
  */
trait CSVConnector extends Logging {

  val spark: SparkSession
  val path: String
  val inferSchema: String = "true"
  val delimiter: String = ";"
  val header: String = "true"

  /**
    * Read a [[DataFrame]] from a csv file with the path defined during the instantiation.
    *
    * @return
    */
  protected def readCSV(): DataFrame = {
  log.debug(s"Reading CSV file from $path")
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
    log.debug(s"Write DataFrame to $path in CSV format")
    df.write
      .mode(saveMode)
      .option("header", this.header)
      .option("delimiter", this.delimiter)
      .csv(path)
  }
}
