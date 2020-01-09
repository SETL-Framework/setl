package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.DataFrame

@InterfaceStability.Evolving
abstract class DBConnector extends Connector with HasSparkSession {
  @deprecated("This method will be removed in v0.4. Use `create(t: DataFrame)` instead.", "0.3.0")
  def create(t: DataFrame, suffix: Option[String]): Unit

  def create(t: DataFrame): Unit

  def delete(query: String): Unit

  def drop(): Unit
}
