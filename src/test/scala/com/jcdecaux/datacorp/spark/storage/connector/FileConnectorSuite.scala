package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FunSuite

import scala.util.Random

class FileConnectorSuite extends FunSuite {

  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
  val path: String = "src/test/resources/test_csv"

  val connector: FileConnector = new FileConnector(spark, Map[String, String]("path" -> "src/test/resources")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  val connector2: FileConnector = new FileConnector(spark, Map[String, String]("path" -> "src/test/resources", "filenamePattern" -> "(test-json).*")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  test("File connector list files ") {
    assert(connector.listFiles().length > 1)
    assert(connector2.listFiles().length === 1)
  }

  test("File connector functionality") {
    assert(connector2.getSize === 624)
  }

  test("FileConnector should throw exception with we try add suffix to an already-saved non-suffix directory") {
    import spark.implicits._
    val connector: FileConnector =
      new FileConnector(spark, Map[String, String]("path" -> (path + "suffix_handling_exception"), "filenamePattern" -> "(test).*")) {
        override val storage: Storage = Storage.CSV

        override def read(): DataFrame = null

        //        override def write(t: DataFrame): Unit = {
        //          this.writeCount.getAndAdd(1)
        //        }
      }

    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    connector.write(dff.toDF, None)
    assertThrows[IllegalArgumentException](connector.write(dff.toDF, Some("test")))
    assertThrows[IllegalArgumentException](connector.setSuffix(Some("test")))
    connector.delete()
  }

  test("FileConnector should handle parallel write") {
    import spark.implicits._

    val connector: FileConnector = new FileConnector(spark, Map[String, String](
      "path" -> "src/test/resources/test_csv_parallel",
      "inferSchema" -> "true",
      "header" -> "false",
      "saveMode" -> "Overwrite",
      "storage" -> "CSV"
    )) {
      override val storage: Storage = Storage.CSV

      override def write(t: DataFrame): Unit = {
        writeToPath(t, outputPath)
        Thread.sleep(Random.nextInt(200))
      }
    }

    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val suffixes = (1 to 100).map(_.toString).toList

    val header: String = null

    try {
      (header :: suffixes).par
        .foreach({
          x => connector.write(dff.toDF(), Option(x))
        })

      assert(connector.getWriteCount === suffixes.size + 1)
      ("default" :: suffixes).par
        .foreach {
          x =>
            val data = spark.read.csv(s"src/test/resources/test_csv_parallel/_user_defined_suffix=${x}")
            assert(data.count() === 6, s"the file src/test/resources/test_csv_parallel/_user_defined_suffix=${x} should have 6 rows")
        }
    } catch {
      case e: IllegalArgumentException => //
      case other: Exception => throw other
    }

    connector.delete()
  }

}
