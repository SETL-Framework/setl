package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.config.{Conf, DeltaConnectorConf}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * DeltaConnector contains functionality for transforming [[DataFrame]] into DeltaLake files
 */
class DeltaConnector(val options: DeltaConnectorConf) extends ACIDConnector  {

  val storage = Storage.DELTA

  /**
   * Partition columns when writing the data frame
   */
  private[this] val partition: ArrayBuffer[String] = ArrayBuffer()

  private[this] val _recursive: Boolean = true

  def this(options: Map[String, String]) = this(DeltaConnectorConf.fromMap(options))

  def this(path: String, saveMode: SaveMode) = this(Map("path" -> path, "saveMode" -> saveMode.toString))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  override val reader: DataFrameReader = {
    spark.read
      .format("delta")
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .mode(options.getSaveMode)
      .options(options.getWriterConf)
  }

  @throws[java.io.FileNotFoundException](s"${options.getPath} doesn't exist")
  @throws[org.apache.spark.sql.AnalysisException](s"${options.getPath} doesn't exist")
  override def read(): DataFrame = {
    log.debug(s"Reading ${storage.toString} file in: '${options.getPath}'")
    this.setJobDescription(s"Read file(s) from '${options.getPath}'")
    reader.load(options.getPath)
  }

  override def write(df: DataFrame): Unit = {
    this.setJobDescription(s"Write file to ${options.getPath}")
    writer(df)
      .partitionBy(partition: _*)
      .format(storage.toString.toLowerCase())
      .save(options.getPath)
  }

  override def update(df: DataFrame, column: String, columns: String *): Unit = {
    DeltaTable.forPath(options.getPath).as("oldData")
      .merge(
        df.toDF().as("newData"),
        (columns :+ column).map(col => s"oldData.$col = newData.$col").mkString(" AND ")
      )
      .whenMatched
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    if (suffix.isDefined) log.warn("Suffix is not supported in DeltaConnector")
    write(t)
  }

  override def delete(query: String): Unit = {
    this.delete(expr(query))
  }

  def delete(condition: Column): Unit = {
    DeltaTable.forPath(options.getPath).delete(condition)
  }

  override def drop(): Unit = {
    log.debug(s"Delete ${options.getPath}")
    FileSystem
      .get(spark.sparkContext.hadoopConfiguration)
      .delete(new Path(options.getPath), _recursive)
  }

  def partitionBy(column: String, columns: String*): this.type = {
    log.debug(s"Delta files will be partitioned by ${columns.mkString(", ")}")
    partition.append(column)
    partition.append(columns: _*)
    this
  }

  override def vacuum(retentionHours: Double): Unit = {
    DeltaTable.forPath(options.getPath).vacuum(retentionHours)
  }

  override def vacuum(): Unit = {
    DeltaTable.forPath(options.getPath).vacuum()
  }



}
