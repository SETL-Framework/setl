package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.spark.sql.DataFrame

/**
  * An enrich connector has further more data manipulation functionality
  */
@InterfaceStability.Evolving
trait EnrichConnector extends Connector {

  def create(t: DataFrame): Unit

  def delete(query: String): Unit
}
