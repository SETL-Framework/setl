package com.jcdecaux.setl.util

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession

trait HasSparkSession {

  val spark: SparkSession = SparkSession.getActiveSession match {
    case Some(ss) => ss
    case _ => throw new SparkException("No active Spark session")
  }

  def setJobDescription(desc: String): Unit = spark.sparkContext.setJobDescription(desc)

  def setJobGroup(group: String): Unit = spark.sparkContext.setJobGroup(group, null)

}
