package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CSVConnector contains functionality for transforming [[DataFrame]] into csv files
  */
class CSVConnector(val spark: SparkSession,
                   val path: String,
                   val inferSchema: String,
                   val delimiter: String,
                   val header: String,
                   val saveMode: SaveMode) extends Connector with Logging {

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path = ConfigUtils.getAs[String](config, "path").get,
    inferSchema = ConfigUtils.getAs[String](config, "inferSchema").get,
    delimiter = ConfigUtils.getAs[String](config, "delimiter").get,
    header = ConfigUtils.getAs[String](config, "header").get,
    saveMode = SaveMode.valueOf(ConfigUtils.getAs[String](config, "saveMode").get)
  )

  /**
    * Read a [[DataFrame]] from a csv file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from $path")
    this.spark.read
      .option("header", this.header)
      .option("inferSchema", this.inferSchema)
      .option("delimiter", this.delimiter)
      .csv(path)
  }

  /**
    * Write a [[DataFrame]] into the default path with the given save mode
    */
  override def write(df: DataFrame): Unit = {
    this.writeCSV(df, this.path, saveMode)
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[this] def writeCSV(df: DataFrame, path: String, saveMode: SaveMode): Unit = {
    log.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .option("header", this.header)
      .option("delimiter", this.delimiter)
      .csv(path)
  }
}
