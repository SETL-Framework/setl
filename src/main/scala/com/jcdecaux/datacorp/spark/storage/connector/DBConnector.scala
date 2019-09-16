package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.spark.sql.DataFrame

@InterfaceStability.Evolving
abstract class DBConnector extends Connector {
  @deprecated("This method will be removed in v0.4. Use `create(t: DataFrame)` instead.")
  def create(t: DataFrame, suffix: Option[String]): Unit

  def create(t: DataFrame): Unit

  def delete(query: String): Unit
}
