package io.github.setl.storage.connector

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ConnectorSuite extends AnyFunSuite with BeforeAndAfterAll {

  test("Connector object") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()

    val df = spark.emptyDataFrame

    assert(Connector.empty.spark === null)
    assert(Connector.empty.storage === null)
    assert(Connector.empty.read() === null)
    Connector.empty.write(df)
    Connector.empty.write(df, Some("suffix"))
  }

}
