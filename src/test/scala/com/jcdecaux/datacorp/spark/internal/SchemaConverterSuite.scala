package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.exception.InvalidSchemaException
import com.jcdecaux.datacorp.spark.internal.TestClasses.{MyObject, TestCompoundKey, TestNullableColumn}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class SchemaConverterSuite extends FunSuite {
  val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

  import spark.implicits._

  val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()


  test("SchemaConverter should handle the annotation ColumnName") {

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

  test("Schema converter should handle the annotation CompoundKey") {

    val ds = Seq(
      TestCompoundKey("a", 1, "A"),
      TestCompoundKey("b", 2, "B"),
      TestCompoundKey("c", 3, "C")
    ).toDS()

    val df = SchemaConverter.toDF(ds)
    df.show()
    assert(df.columns === Array("a", "b", "c", "_sort_key", "_primary_key"))
    assert(df.collect().map(_.getAs[String]("_primary_key")) === Array("a-1", "b-2", "c-3"))
    assert(df.filter($"_primary_key" === "c-3").collect().length === 1)

    val ds2 = SchemaConverter.fromDF[TestCompoundKey](df)
    ds2.show()
    assert(ds2.columns sameElements Array("a", "b", "c"))
    assert(df.count() === ds2.count())

  }

  test("Schema converter should add missing nullable columns in the DF-DS conversion") {

    val data = Seq(
      TestNullableColumn("A", "a", Some(1), 1D),
      TestNullableColumn("B", "b", Some(2), 2D)
    )

    val ds = data.toDS()

    val df = ds.drop("col3", "col2")

    val data2 = SchemaConverter.fromDF[TestNullableColumn](df).collect()
    assert(data2 === Seq(
      TestNullableColumn("A", null, None, 1D),
      TestNullableColumn("B", null, None, 2D)
    ))
  }

  test("Schema converter should throw exception if any non-nullable column is missing in a DF") {
    val data = Seq(
      TestNullableColumn("A", "a", Some(1), 1D),
      TestNullableColumn("B", "b", Some(2), 2D)
    )

    val ds = data.toDS()

    val df = ds.drop("col4")
    val df2 = ds.drop("col1")

    assertThrows[InvalidSchemaException](SchemaConverter.fromDF[TestNullableColumn](df))
    assertThrows[InvalidSchemaException](SchemaConverter.fromDF[TestNullableColumn](df2))
  }

}

