package com.jcdecaux.setl.storage.connector

import java.io.{ByteArrayOutputStream, File}
import java.sql.{Date, Timestamp}

import com.jcdecaux.setl.config.{Conf, Properties}
import com.jcdecaux.setl.storage.SparkRepositorySuite
import com.jcdecaux.setl.{SparkSessionBuilder, TestObject, TestObject2}
import org.apache.commons.lang.SystemUtils
import org.apache.log4j.{Logger, SimpleLayout, WriterAppender}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExcelConnectorSuite extends AnyFunSuite with Matchers {

  override def withFixture(test: NoArgTest): Outcome = {
    // Shared setup (run at beginning of each test)
    SparkSession.getActiveSession match {
      case Some(ss) => ss.stop()
      case _ =>
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try test()
    finally {
      // Shared cleanup (run at end of each test)
      SparkSession.getActiveSession match {
        case Some(ss) => ss.stop()
        case _ =>
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

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

    val connector3 = new ExcelConnector(conf)
    assert(connector3.read().count() === 3)

    val connector4 = new ExcelConnector(Properties.excelConfig)

    testTable.toDF.show()
    connector4.write(testTable.toDF)

    val df4 = connector4.read()

    df4.show()
    assert(df4.count() === 3)

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

  test("ExcelConnector's sheetName option doesn't work") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val logger = Logger.getLogger(classOf[ExcelConnector])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)
    val warnMessage = "The option `sheetName` is ignored. Use dataAddress"
    
    val conf = new Conf()
      .set("path", "src/test/resources/test_excel_sheet.xlsx")
      .set("sheetName", "testSheet")
      .set("useHeader", "true")
      .set("inferSchema", "true")
      .set("saveMode", "Overwrite")

    val connector = new ExcelConnector(conf)
    assert(outContent.toString.contains(warnMessage))
    connector.write(testTable.toDF)

    assertThrows[IllegalArgumentException](
      spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", "testSheet!A1")
        .option("useHeader", "true")
        .option("inferSchema", "true")
        .load("src/test/resources/test_excel_sheet.xlsx")
        .show()
    )

    if (!SystemUtils.IS_OS_WINDOWS) {
      // the previous assertThrows will cause the file be locked in windows. Skip the delete if OS is windows
      deleteRecursively(new File("src/test/resources/test_excel_sheet.xlsx"))
    }
  }

  test("ExcelConnector with multiple sheets") {
    /*
    We test the ExcelConnector by creating a file with multiple sheets with different save mode.

    If write mode is set to Append:
      - when the file doesn't contains the current sheet, a new sheet will be created in the file and data
        will be written
      - when the file already contains the current sheet, then this sheet will be overwritten
     */
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._

    val thisPath = "src/test/resources/test_excel_with_multiple_sheets.xlsx"

    // create new file and save to testSheet
    val conf1 = new Conf()
      .set("path", thisPath)
      .set("dataAddress", "testSheet!A1")
      .set("useHeader", "true")
      .set("inferSchema", "true")
      .set("saveMode", "Overwrite")

    // append testSheet2 to file
    val conf2 = new Conf()
      .set("path", thisPath)
      .set("dataAddress", "testSheet2!A1")
      .set("useHeader", "true")
      .set("inferSchema", "true")
      .set("saveMode", "Append")


    val testTable1: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(1, "p1", "c1", 1L)
    ).toDS()

    val testTable2: Dataset[TestObject] = Seq(
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val connector1 = new ExcelConnector(conf1)
    val connector2 = new ExcelConnector(conf2)

    connector1.write(testTable1.toDF())
    connector2.write(testTable2.toDF())

    // create file
    assert(connector1.read().count() === 2)
    connector1.read()
        .select(
          $"partition1".cast(IntegerType).as[Int],
          $"partition2".as[String],
          $"clustering1".as[String],
          $"value".cast(LongType).as[Long]
        )
        .as[TestObject]
        .collect() should contain theSameElementsAs testTable1.collect()

    // add data to testSheet2
    assert(connector2.read().count() === 1)
    connector2.read()
      .select(
        $"partition1".cast(IntegerType).as[Int],
        $"partition2".as[String],
        $"clustering1".as[String],
        $"value".cast(LongType).as[Long]
      )
      .as[TestObject]
      .collect() should contain theSameElementsAs testTable2.collect()

    // Append to an existing testSheet2 should overwrite it
    connector2.write(testTable1.toDF())
    assert(connector2.read().count() === 2)
    connector2.read()
      .select(
        $"partition1".cast(IntegerType).as[Int],
        $"partition2".as[String],
        $"clustering1".as[String],
        $"value".cast(LongType).as[Long]
      )
      .as[TestObject]
      .collect() should contain theSameElementsAs testTable1.collect()

    // Overwrite the whole file
    connector1.write(testTable2.toDF())
    assert(connector1.read().count() === 1)
    connector1.read()
      .select(
        $"partition1".cast(IntegerType).as[Int],
        $"partition2".as[String],
        $"clustering1".as[String],
        $"value".cast(LongType).as[Long]
      )
      .as[TestObject]
      .collect() should contain theSameElementsAs testTable2.collect()

    // testSheet2 is gone
    assertThrows[IllegalArgumentException](connector2.read().show())

    if (!SystemUtils.IS_OS_WINDOWS) {
      // the previous assertThrows will cause the file be locked in windows. Skip the delete if OS is windows
      deleteRecursively(new File("src/test/resources/test_excel_sheet.xlsx"))
    }  }
}
