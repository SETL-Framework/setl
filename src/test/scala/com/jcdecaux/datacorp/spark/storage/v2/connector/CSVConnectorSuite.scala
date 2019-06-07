package com.jcdecaux.datacorp.spark.storage.v2.connector

import java.io.File

import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class CSVConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  val path: String = "src/test/resources/test_csv"

  val csvConnector = new CSVConnector(spark, path, "true", "|", "true", SaveMode.Append)

  import SparkRepositorySuite.deleteRecursively

  test("IO") {

    import spark.implicits._
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    testTable.toDF.show()
    csvConnector.write(testTable.toDF)
    csvConnector.write(testTable.toDF)

    val df = csvConnector.read()

    df.show()
    assert(df.count() === 6)
    deleteRecursively(new File(path))

  }

  test("IO with auxiliary constructor") {
    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val connector = new CSVConnector(spark, Properties.csvConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    deleteRecursively(new File("src/test/resources/test_config_csv"))
  }
}
