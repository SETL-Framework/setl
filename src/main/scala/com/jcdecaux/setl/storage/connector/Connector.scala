package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.internal.Logging
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql._

/**
 * A connector it a fundamental element to access a data persistence store.
 * It could be used by a [[com.jcdecaux.setl.storage.repository.Repository]].
 *
 * <br>
 * A basic data storage connector has two main function:
 * <ul>
 * <li>Read data from the persistence store</li>
 * <li>Write data into the persistence store</li>
 * </ul>
 *
 */
@InterfaceStability.Evolving
trait Connector extends HasSparkSession with Logging {

  val storage: Storage

  protected val reader: DataFrameReader

  protected val writer: DataFrame => DataFrameWriter[Row]

  def read(): DataFrame

  def write(t: DataFrame, suffix: Option[String]): Unit

  def write(t: DataFrame): Unit

}

object Connector {

  def empty: Connector = new Connector {
    override val spark: SparkSession = null
    override val storage: Storage = null
    override val reader: DataFrameReader = null
    override val writer: DataFrame => DataFrameWriter[Row] = null
    override def read(): DataFrame = null
    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
    override def write(t: DataFrame): Unit = {}
  }

}
