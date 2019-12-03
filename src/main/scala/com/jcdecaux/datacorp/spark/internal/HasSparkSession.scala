package com.jcdecaux.datacorp.spark.internal

import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val spark: SparkSession = SparkSession.getActiveSession.get

}
