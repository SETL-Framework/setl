package com.jcdecaux.datacorp.spark.storage.v2.connector

import java.io.File
import java.sql.{Date, Timestamp}

import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject2}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class ExcelConnectorSuite extends FunSuite {

  import SparkRepositorySuite.deleteRecursively

  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  val path: String = "src/test/resources/test_excel.xlsx"

  import spark.implicits._

  val testTable: Dataset[TestObject2] = Seq(
    TestObject2("string", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string2", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string3", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L)
  ).toDS()

  test("IO with default parameters") {
    val excelConnector = new ExcelConnector(spark, path, "true")

    testTable.toDF.show(false)
    excelConnector.write(testTable.toDF)

    val df = excelConnector.read()

    df.show(false)
    df.printSchema()
    assert(df.count() === 3)
    assert(df.head.getAs[String]("col4") === "2019-05-06 14:34:28.000")
    assert(df.head.getAs[String]("col5") === "2019-05-06")
    deleteRecursively(new File(path))

  }

  test("IO with customized format") {
    val schema: StructType = StructType(Array(
      StructField("col1", StringType),
      StructField("col2", IntegerType),
      StructField("col3", DoubleType),
      StructField("col4", TimestampType),
      StructField("col5", DateType),
      StructField("col6", LongType)
    ))

    val excelConnector = new ExcelConnector(
      spark,
      path,
      useHeader = "true",
      timestampFormat = "dd/mm/yyyy hh:mm:ss",
      dateFormat = "dd/mm/yy",
      schema = Some(schema)
    )

    excelConnector.write(testTable.toDF)
    excelConnector.write(testTable.toDF)

    val df = excelConnector.read()

    df.show(false)
    df.printSchema()
    assert(df.head.getAs[Timestamp]("col4") === new Timestamp(1557153268000L))
    assert(df.head.getAs[Date]("col5").getTime === 1557100800000L)

    deleteRecursively(new File(path))

  }

}
