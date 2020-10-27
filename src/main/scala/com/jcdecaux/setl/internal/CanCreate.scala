package com.jcdecaux.setl.internal

import com.jcdecaux.setl.storage.connector.Connector
import org.apache.spark.sql.DataFrame

/**
 * Connectors that inherit CanCreate should be able to create a table in a database or a file/folder in a file system
 */
trait CanCreate {
  self: Connector =>

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system) with a suffix
   *
   * @param t      data frame to be written
   * @param suffix suffix to be appended at the end of the data storage name
   */
  def create(t: DataFrame, suffix: Option[String]): Unit

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system)
   *
   * @param t data frame to be written
   */
  def create(t: DataFrame): Unit

}
