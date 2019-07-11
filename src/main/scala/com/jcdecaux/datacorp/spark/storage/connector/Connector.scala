package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}

/**
  * A connector it a fundamental element to access a data persistence store.
  * It could be used by a [[com.jcdecaux.datacorp.spark.storage.repository.Repository]].
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
trait Connector {

  private[connector] var lastWriteHashCode: Int = -1432561

  val storage: Storage

  var reader: DataFrameReader

  var writer: DataFrameWriter[Row]

  def read(): DataFrame

  def write(t: DataFrame, suffix: Option[String] = None): Unit
}
