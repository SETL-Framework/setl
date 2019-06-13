package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotations.colName
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class SchemaConverterSuite extends FunSuite {
  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()

  import spark.implicits._

  val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()


  test("Test schema converter") {

    ds.show()
    assert(ds.columns === Array("column1", "column2"))

    val df = SchemaConverter.toDF(ds)
    df.show()
    assert(df.columns === Array("col1", "column2"))

    val ds2 = SchemaConverter.fromDF[MyObject](df)
    ds2.show()
    assert(ds2.columns === Array("column1", "column2"))

    val ds3 = SchemaConverter.fromDF[MyObject](ds.toDF())
    ds3.show()
    assert(ds2.columns === Array("column1", "column2"))

  }
}

case class MyObject(@colName("col1") column1: String, column2: String)
