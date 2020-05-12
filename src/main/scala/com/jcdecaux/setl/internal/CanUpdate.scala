package com.jcdecaux.setl.internal

import org.apache.spark.sql.DataFrame

trait CanUpdate {

  def update(df: DataFrame, column: String, columns: String*): Unit

}
