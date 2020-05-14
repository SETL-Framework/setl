package com.jcdecaux.setl.internal

import org.apache.spark.sql.DataFrame

trait CanCreate {

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
