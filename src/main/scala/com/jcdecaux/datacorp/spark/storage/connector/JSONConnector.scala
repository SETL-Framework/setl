package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}

class JSONConnector(override val spark: SparkSession,
                    override val options: Map[String, String]) extends FileConnector(spark, options) {

  override val storage: Storage = Storage.JSON

  override def read(): DataFrame = ???

  override def write(t: DataFrame, suffix: Option[String]): Unit = ???
}
