package com.jcdecaux.datacorp.spark.storage.v2.connector

import org.apache.spark.sql.DataFrame

/**
  * An enrich connector has further more data manipulation functionality
  */
trait EnrichConnector extends Connector {
  def create(t: DataFrame): Unit
  def delete(query: String): Unit
}
