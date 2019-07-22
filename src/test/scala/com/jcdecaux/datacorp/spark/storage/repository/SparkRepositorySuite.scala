package com.jcdecaux.datacorp.spark.storage.repository

import java.io.File

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey}
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.storage.Condition
import com.jcdecaux.datacorp.spark.storage.connector.{CSVConnector, ParquetConnector}
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class SparkRepositorySuite extends FunSuite {

  import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite.deleteRecursively

  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
  val path: String = "src/test/resources/test_parquet"
  val table: String = "test_table"

  val parquetConnector = new ParquetConnector(spark, path, SaveMode.Overwrite)

  import spark.implicits._

  val options = Map[String, String](
    "inferSchema" -> "false",
    "delimiter" -> ";",
    "header" -> "true"
  )

  val testTable: Dataset[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  ).toDS()

  test("Instantiation") {

    val repo = new SparkRepository[TestObject]().setConnector(parquetConnector)
    val condition = Condition("partition1", "=", 1L)

    assert(repo.getStorage === Storage.PARQUET)
    repo.save(testTable)
    repo.save(testTable)
    val test = repo.findAll()
    assert(test.count() === 3)
    assert(repo.getConnector === parquetConnector)

    assert(repo.findBy(condition).count() === 1)
    deleteRecursively(new File(path))

  }

  test("Test with annotated case class") {

    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
    val path: String = "src/test/resources/test_parquet_with_anno"
    val connector = new CSVConnector(spark, Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))
    val condition = Condition("col1", "=", "a")

    val repo = new SparkRepository[MyObject].setConnector(connector)

    repo.save(ds)
    val data = repo.findAll()
    assert(data.columns === Array("column1", "column2"))

    val rawData = connector.read()
    rawData.show()
    assert(rawData.columns === Array("col1", "column2", "_sort_key"))

    val filteredData = repo.findBy(condition)
    assert(filteredData.columns === Array("column1", "column2"))
    assert(filteredData.count() === 1)

    deleteRecursively(new File(path))

  }

  test("Test spark repository save with suffix") {
    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
    val path: String = "src/test/resources/test_parquet_with_annotation"
    val connector = new CSVConnector(spark, Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))

    val repo = new SparkRepository[MyObject].setConnector(connector)

    repo.save(ds, Some("1"))
    repo.save(ds, Some("2"))
    repo.save(ds, Some("3"))
    repo.save(ds, Some("3/4/5"))

    val df = repo.findAll()
    assert(df.count() === 8)

    df.show()
    connector.delete()
  }

}

case class MyObject(@CompoundKey("sort", "2") @ColumnName("col1") column1: String, @CompoundKey("sort", "1") column2: String)
