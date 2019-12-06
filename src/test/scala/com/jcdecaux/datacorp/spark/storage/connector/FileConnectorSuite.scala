package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

import scala.util.Random

class FileConnectorSuite extends FunSuite {

  val path: String = "src/test/resources/test_csv"

  val connector: SparkSession => FileConnector = spark => new FileConnector(Map[String, String]("path" -> "src/test/resources")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  val connector2: SparkSession => FileConnector = spark => new FileConnector(Map[String, String]("path" -> "src/test/resources", "filenamePattern" -> "(test-json).*")) {
    override val storage: Storage = Storage.OTHER

    override def read(): DataFrame = null

    override def write(t: DataFrame, suffix: Option[String]): Unit = {}
  }

  test("File connector list files ") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    assert(connector(spark).listFilesToLoad(false).length === 1)
    assert(connector(spark).listFilesToLoad().length > 1)
    assert(connector(spark).listFiles().length > 1)
    assert(connector2(spark).listFilesToLoad().length === 1)
    assert(connector2(spark).listFiles().length > 1)
    assert(connector(spark).listFiles() === connector2(spark).listFiles())
  }

  test("File connector should handle wildcard file path (parquet)") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val path = "src/test/resources/fileconnector_test_dir"
    import spark.implicits._
    val df = Seq(
      ("a", "A", "aa", "AA"),
      ("a", "A", "bb", "BB"),
      ("a", "B", "aa", "AA"),
      ("a", "B", "bb", "BB"),
      ("b", "A", "aa", "AA"),
      ("b", "A", "bb", "BB"),
      ("b", "B", "aa", "AA"),
      ("b", "B", "bb", "BB")
    ).toDF("col1", "col2", "col3", "col4")

    df.write.mode(SaveMode.Overwrite).partitionBy("col1", "col2").parquet(path)

    val connector = new ParquetConnector("src/test/resources/fileconnector_test_dir/col1=*/col2=A/*", SaveMode.Overwrite)

    val connectorRead = connector.read()

    val sparkRead = spark.read
      .option("basePath", "src/test/resources/fileconnector_test_dir")
      .parquet("src/test/resources/fileconnector_test_dir/col1=*/col2=A/*")

    assert(connector.basePath.toString === "src/test/resources/fileconnector_test_dir")
    assert(connectorRead.collect() === sparkRead.collect())

    // remove test files
    new ParquetConnector(path, SaveMode.Overwrite).delete()
  }

  test("File connector should handle wildcard file path (csv)") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val path = "src/test/resources/fileconnector_test_dir_csv"
    import spark.implicits._
    val df = Seq(
      ("a", "A", "aa", "AA"),
      ("a", "A", "bb", "BB"),
      ("a", "B", "aa", "AA"),
      ("a", "B", "bb", "BB"),
      ("b", "A", "aa", "AA"),
      ("b", "A", "bb", "BB"),
      ("b", "B", "aa", "AA"),
      ("b", "B", "bb", "BB")
    ).toDF("col1", "col2", "col3", "col4")

    df.write.mode(SaveMode.Overwrite).partitionBy("col1", "col2").option("header", "true").csv(path)

    val connector = new CSVConnector(s"$path/col1=*/col2=A/*", "true", ",", "true", SaveMode.Overwrite)
    val connectorRead = connector.read()
    connectorRead.show()

    val sparkRead = spark.read
      .option("basePath", path)
      .option("header", "true")
      .csv(s"$path/col1=*/col2=A/*")
    sparkRead.show()

    assert(connector.basePath.toString === path)
    assert(connectorRead.collect() === sparkRead.collect())

    // remove test files
    new CSVConnector(path, "true", ",", "true", SaveMode.Overwrite).delete()
  }

  test("File connector functionality") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    assert(connector2(spark).getSize === 624)
  }

  test("FileConnector should throw exception with we try add suffix to an already-saved non-suffix directory") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    import spark.implicits._
    val connector: FileConnector =
      new FileConnector(Map[String, String]("path" -> (path + "suffix_handling_exception"), "filenamePattern" -> "(test).*")) {
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
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    import spark.implicits._

    val connector: FileConnector = new FileConnector(Map[String, String](
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
            val data = spark.read.csv(s"src/test/resources/test_csv_parallel/_user_defined_suffix=$x")
            assert(data.count() === 6, s"the file src/test/resources/test_csv_parallel/_user_defined_suffix=$x should have 6 rows")
        }
    } catch {
      case e: IllegalArgumentException => //
      case other: Exception => throw other
    }

    connector.delete()
  }

  test("FileConnector should handle base path correctly") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val connector: FileConnector = new FileConnector(Map[String, String](
      "path" -> "src/test/resources/test_base_path.csv",
      "inferSchema" -> "true",
      "header" -> "false",
      "saveMode" -> "Overwrite",
      "storage" -> "CSV"
    )) {
      override val storage: Storage = Storage.CSV
    }

    assert(connector.basePath.toString !== "src/test/resources/test_base_path.csv")
    assert(connector.basePath.toString === "src/test/resources")
  }

}
