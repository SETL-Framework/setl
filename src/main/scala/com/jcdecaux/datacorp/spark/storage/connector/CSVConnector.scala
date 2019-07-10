package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CSVConnector contains functionality for transforming [[DataFrame]] into csv files
  */
@InterfaceStability.Evolving
class CSVConnector(val spark: SparkSession,
                   val path: String,
                   val inferSchema: String,
                   val delimiter: String,
                   val header: String,
                   val saveMode: SaveMode) extends FileConnector with Logging {

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path = TypesafeConfigUtils.getAs[String](config, "path").get,
    inferSchema = TypesafeConfigUtils.getAs[String](config, "inferSchema").get,
    delimiter = TypesafeConfigUtils.getAs[String](config, "delimiter").get,
    header = TypesafeConfigUtils.getAs[String](config, "header").get,
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").get)
  )

  def this(spark: SparkSession, conf: Conf) = this(
    spark = spark,
    path = conf.get("path").get,
    inferSchema = conf.get("inferSchema").get,
    delimiter = conf.get("delimiter").get,
    header = conf.get("header").get,
    saveMode = SaveMode.valueOf(conf.get("saveMode").get)
  )

  override val storage: Storage = Storage.CSV

  /**
    * Read a [[DataFrame]] from a csv file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from $path")

    val df = this.spark.read
      .option("header", this.header)
      .option("inferSchema", this.inferSchema)
      .option("delimiter", this.delimiter)
      .option("basePath", path)
      .csv(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }

  /**
    * Write a [[DataFrame]] into the default path with the given save mode
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        this.writeCSV(df, s"${this.path}/$userDefinedSuffix=$s", saveMode)
      case _ =>
        checkPartitionValidity(false)
        this.writeCSV(df, this.path, saveMode)
    }
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[this] def writeCSV(df: DataFrame, path: String, saveMode: SaveMode): Unit = {
    log.debug(s"Write DataFrame to $path")
    df.write
      .mode(saveMode)
      .partitionBy(_partition: _*)
      .option("header", this.header)
      .option("delimiter", this.delimiter)
      .csv(path)
  }

}
