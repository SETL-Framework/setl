package com.jcdecaux.datacorp.spark.storage.v2.connector

/**
  * A connector is used by a [[com.jcdecaux.datacorp.spark.storage.v2.repository.Repository]] to access a data persistence store.
  *
  * A basic data storage connector has two main function:
  * <ul>
  * <li>Read data from the persistence store</li>
  * <li>Write data into the persistence store</li>
  * </ul>
  *
  * @tparam T : data type
  */
trait Connector[T] {

  def read(): T

  def write(t: T): Unit

}
