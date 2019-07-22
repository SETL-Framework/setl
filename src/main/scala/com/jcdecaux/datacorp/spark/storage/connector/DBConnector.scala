package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.spark.sql.DataFrame

@InterfaceStability.Evolving
abstract class DBConnector extends Connector {
  def create(t: DataFrame, suffix: Option[String] = None): Unit

  def delete(query: String): Unit
}
