package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.internal.Logging
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql._

/**
 * Connector is a non-typed data access layer (DAL) abstraction that provides read/write functionalities.
 *
 * <br>
 * A basic data storage connector has two main functionalities:
 * <ul>
 * <li>Read data from the persistence store</li>
 * <li>Write data into the persistence store</li>
 * </ul>
 *
 */
@InterfaceStability.Evolving
trait Connector extends HasSparkSession with Logging {

  val storage: Storage

  /**
   * Read data from the data source
   * @return a [[DataFrame]]
   */
  def read(): DataFrame

  /**
   * Write a [[DataFrame]] into the data storage
   * @param t a [[DataFrame]] to be saved
   * @param suffix for data connectors that support suffix (e.g. [[FileConnector]],
   *               add the given suffix to the save path
   */
  def write(t: DataFrame, suffix: Option[String]): Unit

  /**
   * Write a [[DataFrame]] into the data storage
   * @param t a [[DataFrame]] to be saved
   */
  def write(t: DataFrame): Unit

}

object Connector {

  /**
   * Create an empty Connector
   * @return an empty Connector
   */
  def empty: Connector = new Connector {
    override val spark: SparkSession = null
    override val storage: Storage = null
    override def read(): DataFrame = null
    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
    override def write(t: DataFrame): Unit = {}
  }

}
