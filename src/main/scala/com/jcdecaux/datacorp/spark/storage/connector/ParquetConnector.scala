package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * ParquetConnector contains functionality for transforming [[DataFrame]] into parquet files
  */
@InterfaceStability.Evolving
class ParquetConnector(val spark: SparkSession,
                       val path: String,
                       val table: String,
                       val saveMode: SaveMode) extends FileConnector with Logging {

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path = TypesafeConfigUtils.getAs[String](config, "path").get,
    table = TypesafeConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").get)
  )

  def this(spark: SparkSession, conf: Conf) = this(
    spark = spark,
    path = conf.get("path").get,
    table = conf.get("table").get,
    saveMode = SaveMode.valueOf(conf.get("saveMode").get)
  )

  override val storage: Storage = Storage.PARQUET

  /**
    * Read a [[DataFrame]] from a parquet file with the path defined during the instantiation
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from $path")
    val df = this.spark.read
      .option("basePath", path)
      .parquet(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }

  /**
    * Write a [[DataFrame]] into parquet file
    *
    * @param df dataframe to be written
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        writeParquet(df, s"${this.path}/$userDefinedSuffix=$s", saveMode)
      case _ =>
        checkPartitionValidity(false)
        writeParquet(df, path, saveMode)
    }
  }

  private[this] def writeParquet(df: DataFrame, absolutePath: String, mode: SaveMode): Unit = {
    log.debug(s"Write DataFrame to $absolutePath")
    df.write
      .mode(mode)
      .partitionBy(_partition: _*)
      .option("path", absolutePath)
      .saveAsTable(table)
  }

}
