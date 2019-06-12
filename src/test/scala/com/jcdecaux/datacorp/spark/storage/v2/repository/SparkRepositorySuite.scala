package com.jcdecaux.datacorp.spark.storage.v2.repository

import java.io.File

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.storage.Condition
import com.jcdecaux.datacorp.spark.storage.v2.connector.ParquetConnector
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class SparkRepositorySuite extends FunSuite {

  import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite.deleteRecursively

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

}
