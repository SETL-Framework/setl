package com.jcdecaux.datacorp.spark.storage.v2.connector

import org.apache.spark.sql.DataFrame

/**
  * A connector is used by a [[com.jcdecaux.datacorp.spark.storage.v2.repository.Repository]] to access a data persistence store.
  *
  * A basic data storage connector has two main function:
  * <ul>
  * <li>Read data from the persistence store</li>
  * <li>Write data into the persistence store</li>
  * </ul>
  *
  */
trait Connector {

  def read(): DataFrame

  def write(t: DataFrame): Unit
}
