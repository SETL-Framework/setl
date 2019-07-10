package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class ParquetConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  val path: String = "src/test/resources/test_parquet"
  val table: String = "test_table"

  val parquetConnector = new ParquetConnector(spark, path, table, SaveMode.Overwrite)

  import spark.implicits._

  val testTable: Dataset[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  ).toDS()

  test("parquet connector  IO ") {

    testTable.toDF.show()
    parquetConnector.write(testTable.toDF())
    parquetConnector.write(testTable.toDF())

    val df = parquetConnector.read()
    df.show()
    assert(df.count() === 3)
    parquetConnector.delete()
  }

  test("IO with auxiliary parquet connector constructor") {

    val connector = new ParquetConnector(spark, Properties.parquetConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    connector.delete()
  }

  test("Test Parquet Connector Suffix") {

    parquetConnector.write(testTable.toDF(), Some("2"))
    parquetConnector.write(testTable.toDF(), Some("1"))
    parquetConnector.write(testTable.toDF(), Some("3"))

    val df = parquetConnector.read()
    df.show()
    assert(df.count() == 9)
    assert(df.filter($"partition1" === 1).count() === 3)
    assert(df.filter($"partition1" === 1).dropDuplicates().count() === 1)

    parquetConnector.delete()
    assertThrows[java.io.FileNotFoundException](parquetConnector.read())
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

    val parquetConnector2 = new ParquetConnector(spark, path, table, SaveMode.Overwrite)
      .partitionBy("partition1", "partition2")

    // with partition, with suffix
    parquetConnector2.write(dff.toDF, Some("1"))
    parquetConnector2.write(dff.toDF, Some("2"))
    parquetConnector2.dropUserDefinedSuffix = false

    assertThrows[IllegalArgumentException](parquetConnector2.write(dff.toDF))

    parquetConnector2.read().show()
    assert(parquetConnector2.read().count() === 12)
    assert(parquetConnector2.read().columns.length === 5)
    parquetConnector2.delete()

    // with partition without suffix
    parquetConnector2.write(dff.toDF)
    assert(parquetConnector2.read().count() === 6)
    assert(parquetConnector2.read().columns.length === 4, "column suffix should not exists")
    parquetConnector2.dropUserDefinedSuffix = true
    assert(parquetConnector2.read().columns.length === 4, "column suffix should not exists")
    parquetConnector2.delete()
  }

}
