package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotations.{ColumnName, CompoundKey}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class SchemaConverterSuite extends FunSuite {
  val spark: SparkSession = new SparkSessionBuilder().setEnv("dev").build().get()

  import spark.implicits._

  val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()


  test("Test ColumnName") {

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

  test("Test CompoundKey") {

    val ds = Seq(
      TestCompoundKey("a", 1, "A"),
      TestCompoundKey("b", 2, "B"),
      TestCompoundKey("c", 3, "C")
    ).toDS()

    val df = SchemaConverter.toDF(ds)
    df.show()
    assert(df.columns === Array("a", "b", "c", "_key"))
    assert(df.collect().map(_.getAs[String]("_key")) === Array("A-a-1", "B-b-2", "C-c-3"))
    assert(df.filter($"_key" === "C-c-3").collect().length === 1)

    val ds2 = SchemaConverter.fromDF[TestCompoundKey](df)
    ds2.show()
    assert(ds2.columns === Array("a", "b", "c"))
    assert(df.count() === ds2.count())

  }

}

case class MyObject(@ColumnName("col1") column1: String, column2: String)

case class TestCompoundKey(@CompoundKey("2") a: String, @CompoundKey("3") b: Int, @CompoundKey("1") c: String)