package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.util.HasSparkSession
import org.apache.spark.sql.DataFrame

@InterfaceStability.Evolving
abstract class DBConnector extends Connector with HasSparkSession {
  @deprecated("This method will be removed in v0.4. Use `create(t: DataFrame)` instead.", "0.3.0")
  def create(t: DataFrame, suffix: Option[String]): Unit

  def create(t: DataFrame): Unit

  def delete(query: String): Unit
}
