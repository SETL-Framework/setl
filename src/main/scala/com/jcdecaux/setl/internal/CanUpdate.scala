package com.jcdecaux.setl.internal

import org.apache.spark.sql.DataFrame

trait CanUpdate {

  /**
   * Update the data store with a new data frame and the given matching columns.
   *
   * All the matched data will be updated, the non-matched data will be inserted
   *
   * @param df new data
   * @param columns other columns to be matched
   */
  def update(df: DataFrame, columns: String*): Unit

}
