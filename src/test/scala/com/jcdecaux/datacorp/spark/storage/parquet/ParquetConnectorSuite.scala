package com.jcdecaux.datacorp.spark.storage.parquet

import java.io.File

import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite
import com.jcdecaux.datacorp.spark.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.FunSuite

class ParquetConnectorSuite extends FunSuite with ParquetConnector {

  import SparkRepositorySuite.deleteRecursively

  override val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()
  override val path: String = "src/test/resources/test_parquet"
  override val table: String = "test_table"

  test("IO") {

    import spark.implicits._
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    testTable.toDF.show()
    this.writeParquet(testTable.toDF(), SaveMode.Overwrite)

    val df = this.readParquet()
    df.show()
    assert(df.count() === 3)
    deleteRecursively(new File(path))

  }
}
