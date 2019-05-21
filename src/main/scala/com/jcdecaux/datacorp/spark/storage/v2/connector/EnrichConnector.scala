package com.jcdecaux.datacorp.spark.storage.v2.connector

/**
  * An enrich connector has further more data manipulation functionality
  *
  * @tparam T data type
  */
trait EnrichConnector[T] extends Connector[T] {
  def create(t: T): Unit
  def delete(query: String): Unit
}
