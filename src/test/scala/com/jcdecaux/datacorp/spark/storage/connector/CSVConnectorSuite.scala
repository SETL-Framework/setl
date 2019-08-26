package com.jcdecaux.datacorp.spark.storage.connector

import java.io.File

import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class CSVConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
  val path: String = "src/test/resources/test_csv"

  val options: Map[String, String] = Map[String, String](
    "path" -> path,
    "inferSchema" -> "true",
    "delimiter" -> "|",
    "header" -> "true",
    "saveMode" -> "Append"
  )

  val csvConnector = new CSVConnector(spark, options)

  import spark.implicits._

  val testTable: Dataset[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  ).toDS()

  test("test CSV connector with different file path") {
    val path1: String = new File("src/test/resources/test_csv").toURI.toString
    val path2: String = new File("src/test/resources/test_csv").getPath

    val csvConnector1 = new CSVConnector(spark, Map[String, String](
      "path" -> path1,
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    ))
    val csvConnector2 = new CSVConnector(spark, Map[String, String](
      "path" -> path2,
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    ))

    csvConnector1.write(testTable.toDF)
    val df = csvConnector2.read()
    assert(df.count() === 3)
    csvConnector.delete()
  }

  test("IO CSVConnector") {

    testTable.toDF.show()
    csvConnector.write(testTable.toDF)
    csvConnector.write(testTable.toDF)

    val df = csvConnector.read()

    df.show()
    assert(df.count() === 6)
    csvConnector.delete()
  }

  test("IO with auxiliary CSVConnector constructor") {
    val connector = new CSVConnector(spark, Properties.csvConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    connector.delete()
  }

  test("Test CSV Connector Suffix") {

    csvConnector.write(testTable.toDF(), Some("2"))
    csvConnector.write(testTable.toDF(), Some("2"))
    csvConnector.write(testTable.toDF(), Some("1"))
    csvConnector.write(testTable.toDF(), Some("3"))

    val df = csvConnector.read()
    df.show()
    assert(df.count() == 12)
    assert(df.filter($"partition1" === 1).count() === 4)
    assert(df.filter($"partition1" === 1).dropDuplicates().count() === 1)

    csvConnector.delete()
    assertThrows[java.io.FileNotFoundException](csvConnector.read())
  }

  test("test partition by") {
    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val csvConnector2 = new CSVConnector(spark, Map[String, String](
      "path" -> path,
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))
      .partitionBy("partition1", "partition2")

    // with partition, with suffix
    csvConnector2.write(dff.toDF, Some("1"))
    csvConnector2.write(dff.toDF, Some("2"))
    csvConnector2.dropUserDefinedSuffix = false

    assertThrows[IllegalArgumentException](csvConnector2.write(dff.toDF))

    csvConnector2.read().show()
    assert(csvConnector2.read().count() === 12)
    assert(csvConnector2.read().columns.length === 5)
    csvConnector2.delete()

    // with partition without suffix
    csvConnector2.write(dff.toDF)
    assert(csvConnector2.read().count() === 6)
    assert(csvConnector2.read().columns.length === 4, "column suffix should not exists")
    csvConnector2.dropUserDefinedSuffix = true
    assert(csvConnector2.read().columns.length === 4, "column suffix should not exists")
    csvConnector2.delete()

  }

  test("Test csv connctor with Schema") {
    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS

    val csvConnectorWithSchema = new CSVConnector(spark, Properties.getConfig("connector.csvWithSchema"))
    val csvConnectorWithSchema2 = new CSVConnector(spark, Properties.getConfig("connector.csvWithSchema2"))

    csvConnectorWithSchema.write(dff.toDF)
    assert(csvConnectorWithSchema.read().columns === Array("partition2", "clustering1", "partition1", "value"))
    csvConnectorWithSchema.delete()

    csvConnectorWithSchema2.write(dff.toDF)
    assert(csvConnectorWithSchema2.read().columns === Array("partition2", "value", "clustering1", "partition1"))
    csvConnectorWithSchema2.delete()
  }
}
