package com.jcdecaux.datacorp.spark.storage.connector

import java.io.File

import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class JSONConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
  val path: String = "src/test/resources/test JSON"

  val connector = new JSONConnector(spark, Map("path" -> path, "saveMode" -> "Overwrite"))

  import spark.implicits._

  val testTable: Dataset[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  ).toDS()

  test("JSON connector IO") {
    connector.partitionBy("partition1").write(testTable.toDF())
    val df = connector.read()
    assert(df.count() === 6)
    assert(df.filter($"partition1" === 2).count() === 4)
    connector.delete()
  }

  test("test JSON connector with different file path") {
    val path1: String = new File("src/test/resources/test_json").toURI.toString
    val path2: String = new File("src/test/resources/test_json").getPath

    val connector1 = new JSONConnector(spark, Map[String, String]("path" -> path1, "saveMode" -> "Overwrite"))
    val connector2 = new JSONConnector(spark, Map[String, String]("path" -> path2, "saveMode" -> "Overwrite"))

    connector1.write(testTable.toDF)
    val df = connector2.read()
    assert(df.count() === 6)
    connector1.delete()
  }

  test("IO with auxiliary JSONConnector constructor") {
    val connector = new JSONConnector(spark, Properties.jsonConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 12)
    connector.delete()
  }

  test("Test JSON Connector Suffix") {

    val connector = new JSONConnector(spark, Map("path" -> path, "saveMode" -> "Append"))

    connector.write(testTable.toDF(), Some("2"))
    connector.write(testTable.toDF(), Some("2"))
    connector.write(testTable.toDF(), Some("1"))
    connector.write(testTable.toDF(), Some("3"))

    val df = connector.read()
    df.show()
    assert(df.count() == 24)
    assert(df.filter($"partition1" === 1).count() === 4)
    assert(df.filter($"partition1" === 1).dropDuplicates().count() === 1)

    connector.delete()
    assertThrows[java.io.FileNotFoundException](connector.read())
  }

  test("test JSON partition by") {
    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val connector = new JSONConnector(spark, Map[String, String]("path" -> path, "saveMode" -> "Overwrite"))
      .partitionBy("partition1", "partition2")

    // with partition, with suffix
    connector.write(dff.toDF, Some("1"))
    connector.write(dff.toDF, Some("2"))
    connector.dropUserDefinedSuffix = false

    connector.read().show()
    assert(connector.read().count() === 12)
    assert(connector.read().columns.length === 5)
    connector.delete()

    // with partition without suffix
    connector.resetSuffix(true)
    connector.write(dff.toDF)
    assert(connector.read().count() === 6)
    assert(connector.read().columns.length === 4, "column suffix should not exists")
    connector.dropUserDefinedSuffix = true
    assert(connector.read().columns.length === 4, "column suffix should not exists")
    connector.delete()
  }

  test("Complex JSON file") {
    /*
    TODO cannot run this test with the current guava version (21.0). This version is a dependency of embedded Cassandra
     IllegalAccessException will be thrown. You should try with version 15.0
     */

    // val connector = new JSONConnector(spark, Map("path" -> "src/test/resources/test-json.json", "saveMode" -> "Append", "multiLine" -> "true"))
    // connector.read().show()

  }

}
