package com.jcdecaux.setl.internal

import org.apache.spark.sql.DataFrame

trait CanCreate {

  def create(t: DataFrame, suffix: Option[String]): Unit

  def create(t: DataFrame): Unit

}
