package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.config.{Conf, DeltaConnectorConf}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.internal.{CanPartition, HasReaderWriter}
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
class DeltaConnector(val options: DeltaConnectorConf) extends ACIDConnector with HasReaderWriter with CanPartition {

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
      .options(options.getReaderConf)
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .mode(options.getSaveMode)
      .options(options.getWriterConf)
  }

  @throws[java.io.FileNotFoundException](s"${options.getPath} doesn't exist")
  @throws[org.apache.spark.sql.AnalysisException](s"${options.getPath} doesn't exist")
  override def read(): DataFrame = {
    logDebug(s"Reading ${storage.toString} file in: '${options.getPath}'")
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

  /**
   * Update the data store with a new data frame and the given matching columns.
   *
   * All the matched data will be updated, the non-matched data will be inserted
   *
   * @param df      new data
   * @param columns columns to be matched
   */
  override def update(df: DataFrame, columns: String*): Unit = {
    DeltaTable.forPath(options.getPath).as("oldData")
      .merge(
        df.toDF().as("newData"),  // TODO @maroil is .toDF() necessary here?
        columns.map(col => s"oldData.$col = newData.$col").mkString(" AND ")
      )
      .whenMatched
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    if (suffix.isDefined) logWarning("Suffix is not supported in DeltaConnector")
    write(t)
  }

  /**
   * Delete rows according to the query
   *
   * @param query a query string
   */
  override def delete(query: String): Unit = {
    this.delete(expr(query))
  }

  /**
   * Delete rows according to the query
   *
   * @param condition a spark column
   */
  def delete(condition: Column): Unit = {
    DeltaTable.forPath(options.getPath).delete(condition)
  }

  /**
   * Drop the entire table.
   */
  override def drop(): Unit = {
    logDebug(s"Delete ${options.getPath}")
    FileSystem
      .get(spark.sparkContext.hadoopConfiguration)
      .delete(new Path(options.getPath), _recursive)
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   * <ul>
   * <li>year=2016/month=01/</li>
   * <li>year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON)
   */
  override def partitionBy(columns: String*): this.type = {
    logDebug(s"Delta files will be partitioned by ${columns.mkString(", ")}")
    partition.append(columns: _*)
    this
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   */
  override def vacuum(retentionHours: Double): Unit = {
    DeltaTable.forPath(options.getPath).vacuum(retentionHours)
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * note: This will use the default retention period of 7 days.
   */
  override def vacuum(): Unit = {
    DeltaTable.forPath(options.getPath).vacuum()
  }

}
