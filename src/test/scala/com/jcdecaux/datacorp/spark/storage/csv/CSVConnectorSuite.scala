package com.jcdecaux.datacorp.spark.storage.csv

import java.io.File

import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class CSVConnectorSuite extends FunSuite with CSVConnector {

  import SparkRepositorySuite.deleteRecursively

  override val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  override val path: String = "src/test/resources/test_csv"

  test("IO") {

    import spark.implicits._
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    testTable.toDF.show()
    this.writeCSV(testTable.toDF, SaveMode.Overwrite)

    val df = this.readCSV()

    df.show()
    assert(df.count() === 3)
    deleteRecursively(new File(path))

  }

  test("test") {
    import spark.implicits._
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val x = testTable.schema.collect({
      case StructField(name, _, _, metadata) if metadata.contains("alias") =>
        col(name).as(metadata.getString("alias"))
      case StructField(name, _, _, _) =>
        col(name)
    })


  }
}
