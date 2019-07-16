package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._

/**
  * CSVConnector contains functionality for transforming [[DataFrame]] into csv files
  */
@InterfaceStability.Evolving
class CSVConnector(override val spark: SparkSession,
                   override val options: Map[String, String]) extends FileConnector(spark, options) {

  override val storage: Storage = Storage.CSV

  def this(spark: SparkSession, path: String, inferSchema: String, delimiter: String, header: String, saveMode: SaveMode) =
    this(spark, Map[String, String](
      "path" -> path,
      "inferSchema" -> inferSchema,
      "header" -> header,
      "saveMode" -> saveMode.toString
    ))

  def this(spark: SparkSession, config: Config) = this(spark = spark, options = TypesafeConfigUtils.getMap(config))

  def this(spark: SparkSession, conf: Conf) = this(spark = spark, options = conf.toMap)

  /**
    * Read a [[DataFrame]] from a csv file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from ${absolutePath.toString}")

    val df = reader.csv(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }

  /**
    * Write a [[DataFrame]] into CSV file
    *
    * @param df     dataframe to be written
    * @param suffix optional, String, write the df in a sub-directory of the defined path
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        this.writeCSV(df, s"${this.absolutePath.toString}/$userDefinedSuffix=$s")
      case _ =>
        checkPartitionValidity(false)
        this.writeCSV(df, this.absolutePath.toString)
    }
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[this] def writeCSV(df: DataFrame, filepath: String): Unit = {
    log.debug(s"Write DataFrame to $filepath")
    initWriter(df)
    writer.csv(filepath)
  }

}
