package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class FileConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

  val connector = new FileConnector(spark, Map[String, String]("path" -> "src/test/resources")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  val connector2 = new FileConnector(spark, Map[String, String]("path" -> "src/test/resources", "filenamePattern" -> "(test).*")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  test("File connector list files ") {
    assert(connector.listFiles().length > 1)
    assert(connector2.listFiles().length === 1)
  }
}
