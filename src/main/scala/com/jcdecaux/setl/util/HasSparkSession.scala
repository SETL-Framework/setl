package com.jcdecaux.setl.util

import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val spark: SparkSession = SparkSession.active

}
