package com.jcdecaux.setl.storage.connector

import java.io.{ByteArrayOutputStream, File}
import java.sql.{Date, Timestamp}

import com.jcdecaux.setl.config.{Conf, Properties}
import com.jcdecaux.setl.storage.SparkRepositorySuite
import com.jcdecaux.setl.{SparkSessionBuilder, TestObject, TestObject2}
import org.apache.log4j.{Logger, SimpleLayout, WriterAppender}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ExcelConnectorSuite extends AnyFunSuite {

  import SparkRepositorySuite.deleteRecursively

  val path: String = "src/test/resources/test_excel.xlsx"

  val testTable: Seq[TestObject2] = Seq(
    TestObject2("string", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string2", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string3", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L)
  )

  test("IO with default excel connector parameters") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

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

  test("IO with customized format excel connector") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._
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
      schema = Some(schema),
      saveMode = SaveMode.Append // TODO SaveMode.Append seems not working.
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

  test("IO with excel connector auxiliary constructor") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val connector = new ExcelConnector(spark, path, "true")
    connector.write(testTable.toDF)
    assert(connector.read().count() === 3)

    val conf = new Conf()
    conf.set("path", path)
    conf.set("useHeader", "true")
    val connector2 = new ExcelConnector(conf)
    assert(connector2.read().count() === 3)

    val connector3 = new ExcelConnector(spark, conf)
    assert(connector3.read().count() === 3)

    val connector4 = new ExcelConnector(Properties.excelConfig)
    val connector5 = new ExcelConnector(spark, Properties.excelConfig)

    testTable.toDF.show()
    connector4.write(testTable.toDF)

    val df4 = connector4.read()
    val df5 = connector5.read()

    df4.show()
    df5.show()
    assert(df4.count() === 3)
    assert(df5.count() === 3)

    deleteRecursively(new File(path))
    deleteRecursively(new File("src/test/resources/test_config_excel.xlsx"))
  }

  test("Infer schema without defining a schema should provoke a warning") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    val logger = Logger.getLogger(classOf[ExcelConnector])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)
    val warnMessage = "Excel connect may not behave as expected when parsing/saving Integers. " +
      "It's recommended to define a schema instead of infer one"

    val conf = new Conf()
    conf.set("path", path)
    conf.set("useHeader", "true")
    conf.set("inferSchema", "true")

    val connector = new ExcelConnector(conf)
    assert(outContent.toString.contains(warnMessage))
    deleteRecursively(new File(path))

    outContent.reset()
    val connector2 = new ExcelConnector(Properties.excelConfigWithoutSchema)
    assert(outContent.toString.contains(warnMessage))
    deleteRecursively(new File("src/test/resources/test_config_excel_without_schema.xlsx"))
  }

  test("IllegalArgumentException should be thrown when writing with save mode different than Append or Overwrite") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val conf = new Conf()
    conf.set("path", path)
    conf.set("useHeader", "true")
    conf.set("inferSchema", "true")
    conf.set("saveMode", "Ignore")

    val connector = new ExcelConnector(conf)
    assertThrows[IllegalArgumentException](connector.write(testTable.toDF()))

    deleteRecursively(new File(path))
  }
}
