package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._

/**
  * ParquetConnector contains functionality for transforming [[DataFrame]] into parquet files
  */
@InterfaceStability.Evolving
class ParquetConnector(val spark: SparkSession,
                       val path: String,
                       //val table: String,
                       val saveMode: SaveMode) extends FileConnector {


  override var reader: DataFrameReader = this.spark.read.option("basePath", basePath)
  override var writer: DataFrameWriter[Row] = _

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path = TypesafeConfigUtils.getAs[String](config, "path").get,
    //table = TypesafeConfigUtils.getAs[String](config, "table").get,
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").get)
  )

  def this(spark: SparkSession, conf: Conf) = this(
    spark = spark,
    path = conf.get("path").get,
    //table = conf.get("table").get,
    saveMode = SaveMode.valueOf(conf.get("saveMode").get)
  )

  override val storage: Storage = Storage.PARQUET

  /**
    * Read a [[DataFrame]] from a parquet file with the path defined during the instantiation
    *
    * @return DataFrame
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from ${absolutePath.toString}")
    val df = reader.parquet(listFiles(): _*)
    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }

  /**
    * Write a [[DataFrame]] into parquet file
    *
    * @param df     dataframe to be written
    * @param suffix optional, String, write the df in a sub-directory of the defined path
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        writeParquet(df, s"${this.absolutePath.toString}/$userDefinedSuffix=$s")
      case _ =>
        checkPartitionValidity(false)
        writeParquet(df, absolutePath.toString)
    }
  }

  private[this] def writeParquet(df: DataFrame, filepath: String): Unit = {
    log.debug(s"Write DataFrame to $filepath")
    initWriter(df)
    writer.parquet(filepath)
    //.option("path", absolutePath)
    // .saveAsTable(table)
  }

  @inline private[this] def initWriter(df: DataFrame): Unit = {
    if (df.hashCode() != lastWriteHashCode) {
      writer = df.write
        .mode(saveMode)
        .partitionBy(partition: _*)
      lastWriteHashCode = df.hashCode()
    }
  }

}
