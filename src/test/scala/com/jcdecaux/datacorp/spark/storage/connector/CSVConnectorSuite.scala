package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class CSVConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  val path: String = "src/test/resources/test_csv"

  val csvConnector = new CSVConnector(spark, path, "true", "|", "true", SaveMode.Append)

  import spark.implicits._

  val testTable: Dataset[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  ).toDS()

  test("IO CSVConnector") {

    testTable.toDF.show()
    csvConnector.write(testTable.toDF)
    csvConnector.write(testTable.toDF)

    val df = csvConnector.read()

    df.show()
    assert(df.count() === 6)
    csvConnector.delete()
  }

  test(s"IO with auxiliary CSVConnector constructor") {
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

    val csvConnector2 = new CSVConnector(spark, path, "true", "|", "true", SaveMode.Overwrite)
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
}
