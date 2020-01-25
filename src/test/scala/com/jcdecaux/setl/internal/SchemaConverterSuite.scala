package com.jcdecaux.setl.internal

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.annotation.{ColumnName, CompoundKey, Compress}
import com.jcdecaux.setl.exception.InvalidSchemaException
import com.jcdecaux.setl.internal.TestClasses._
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaConverterSuite extends AnyFunSuite with Matchers {
  test("SchemaConverter should rename columns according to the annotation ColumnName") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
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

  test("SchemaConverter should only keep the columns defined in the case class") {
    import SchemaConverterSuite._
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val dataWithInferedSchema = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/test_schema_converter.csv")

    assert(SchemaConverter.fromDF[TestClass1](dataWithInferedSchema).columns.length === 4)
    assert(SchemaConverter.fromDF[TestClass2](dataWithInferedSchema).columns.length === 3)
    assert(SchemaConverter.fromDF[TestClass3](dataWithInferedSchema).columns === Array("column1", "column3", "column4"))
    assert(
      SchemaConverter.fromDF[TestClass4](dataWithInferedSchema).columns === Array("column3", "column1", "column4"),
      "columns should be re-ordered"
    )
    assert(
      SchemaConverter.fromDF[TestClass5](dataWithInferedSchema).columns === Array("column3", "column1", "column4"),
      "columns should be re-ordered"
    )
  }

  test("SchemaConverter shouldn always keep the correct column order") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import SchemaConverterSuite._
    import spark.implicits._

    val df = Seq(
      (true, "string", 1, 2D)
    ).toDF("col2", "col1", "col3", "col4")

    assert(SchemaConverter.fromDF[TestOrder](df).collect() === Array(TestOrder("string", true, 1, 2D)))
    assert(SchemaConverter.fromDF[TestOrder2](df).columns === Array("col_1", "col_2", "col_3", "col_4"))
    assert(SchemaConverter.fromDF[TestOrder2](df).collect() === Array(TestOrder2("string", true, 1, 2D)))

    val ds = SchemaConverter.fromDF[TestOrder2](df)
    val renamedDF = SchemaConverter.toDF(ds)
    assert(renamedDF.columns === Array("col1", "col2", "col3", "col4"))

  }

  test("SchemaConverter should throw exception if column can't be found") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import SchemaConverterSuite._
    import spark.implicits._

    val df = Seq(
      (true, "string", 1)
    ).toDF("col2", "col1", "col3")

    assertThrows[com.jcdecaux.setl.exception.InvalidSchemaException](SchemaConverter.fromDF[TestOrder](df))
    assert(SchemaConverter.fromDF[TestException](df).collect() === Array(TestException("string", true, 1, None)))

    assert(SchemaConverter.fromDF[TestException2](df).columns === Array("col_1", "col_2", "col_3", "col_4"))
    assert(SchemaConverter.fromDF[TestException2](df).collect() === Array(TestException2("string", true, 1, None)))
    assertThrows[com.jcdecaux.setl.exception.InvalidSchemaException](SchemaConverter.fromDF[TestException3](df))
  }

  test("SchemaConverter should be able to handle the mix of multiple annotations") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import SchemaConverterSuite._
    import spark.implicits._

    val ds = Seq(
      TestComplex("string", true, 1, 3D)
    ).toDS

    val df = SchemaConverter.toDF(ds)

    val mockDataSource = Seq(
      (true, "string", 1, 3D)
    ).toDF("col2", "col1", "col3", "col4")

    df.show()
    assert(df.columns === Array("col1", "col2", "col3", "col4", "_test_key"))
    assert(SchemaConverter.fromDF[TestComplex](df).collect() === ds.collect())
    assert(SchemaConverter.fromDF[TestComplex](ds.toDF()).collect() === ds.collect())  // should log WARN message
    assert(SchemaConverter.fromDF[TestComplex](mockDataSource).collect() === ds.collect())  // should log WARN message
    assert(SchemaConverter.fromDF[TestComplex](mockDataSource).columns === Array("col_1", "col_2", "col_3", "col_4"))  // should log WARN message

    val ds2 = Seq(
      TestComplex2("string", true, 1, 3D, Seq("string", "haha", "hehe", "hoho"))
    ).toDS

    val df2 = SchemaConverter.toDF(ds2)
    assert(df2.columns === Array("col1", "col2", "col3", "col4", "col5", "_test_key"))
    assert(SchemaConverter.fromDF[TestComplex2](df2).collect() === ds2.collect())
    assert(SchemaConverter.fromDF[TestComplex](df2.toDF()).collect() !== ds2.collect())
  }

  test("Schema converter should handle the annotation CompoundKey") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds = Seq(
      TestCompoundKey("a", 1, "A"),
      TestCompoundKey("b", 2, "B"),
      TestCompoundKey("c", 3, "C")
    ).toDS()

    val df = SchemaConverter.toDF(ds)
    df.show()
    assert(df.columns.toSet === Set("a", "b", "c", "_sort_key", "_primary_key"))
    df.collect().map(_.getAs[String]("_primary_key")) should contain theSameElementsAs Array("a-1", "b-2", "c-3")
    assert(df.filter($"_primary_key" === "c-3").collect().length === 1)

    val ds2 = SchemaConverter.fromDF[TestCompoundKey](df)
    ds2.show()
    ds2.columns should contain theSameElementsAs Array("a", "b", "c")
    assert(df.count() === ds2.count())

  }

  test("[SETL-34] SchemaConverter should handle multi CompoundKeys on the same field") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds = Seq(
      MultipleCompoundKeyTest("a", "1", "A"),
      MultipleCompoundKeyTest("b", "2", "B"),
      MultipleCompoundKeyTest("c", "3", "C")
    ).toDS()

    val df = SchemaConverter.toDF(ds)

    df.columns should equal(Array("col1", "col2", "COLUMN_3", "_part_key", "_sort_key"))
    df.select($"_part_key".as[String]).collect() should contain theSameElementsAs Array("a-A", "b-B", "c-C")
    df.select($"_sort_key".as[String]).collect() should contain theSameElementsAs Array("a-1", "b-2", "c-3")
  }

  test("Schema converter should add missing nullable columns in the DF-DS conversion") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val data = Seq(
      TestNullableColumn("A", "a", Some(1), 1D),
      TestNullableColumn("B", "b", Some(2), 2D)
    )

    val ds = data.toDS()

    val df = ds.drop("col3", "col2")

    val data2 = SchemaConverter.fromDF[TestNullableColumn](df).collect()
    data2 should contain theSameElementsAs Seq(
      TestNullableColumn("A", null, None, 1D),
      TestNullableColumn("B", null, None, 2D)
    )
  }

  test("Schema converter should throw exception if any non-nullable column is missing in a DF") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

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

  test("SchemaConverter should be able to Compress column") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ics = Seq(
      InnerClass("i1", "你好谢谢再见你好谢谢再见你好谢谢再见"),
      InnerClass("i11", "165498465DDDFKLJKSDOIJ__çezé*/-+")
    )

    val test = spark.createDataset(
      Seq(
        TestCompression("col1", "col2", ics, Seq("a", "b", "c")),
        TestCompression("col1", "col2", ics, Seq("a", "b", "c")),
        TestCompression("col1", "col2", ics, Seq("a", "b", "c"))
      )
    )

    val schema = StructAnalyser.analyseSchema[TestCompression]
    val compressed = SchemaConverter.compressColumn(schema)(test.toDF())
    assert(compressed.schema.find(_.name == "col3").get.dataType === BinaryType)
    assert(compressed.schema.find(_.name == "col4").get.dataType === BinaryType)

    val decompressed = SchemaConverter.decompressColumn(schema)(compressed).as[TestCompression]
    assert(test.head === decompressed.head)

    val compressed2 = SchemaConverter.toDF(test)
    compressed2.printSchema()
    assert(compressed2.schema.find(_.name == "col3").get.dataType === BinaryType)
    assert(compressed2.schema.find(_.name == "col4").get.dataType === BinaryType)

    val decompressed2 = SchemaConverter.fromDF[TestCompression](compressed2)
    decompressed2.show()
    assert(test.head === decompressed2.head)
  }
}

object SchemaConverterSuite {

  case class TestClass1(col1: Double, col2: Int, col3: String, col4: String)

  case class TestClass2(col1: Double, col3: String, col4: String)

  case class TestClass3(@ColumnName("col1") column1: Double,
                        @ColumnName("col3") column3: String,
                        @ColumnName("col4") column4: String)

  case class TestClass4(@ColumnName("col3") column3: String,
                        @ColumnName("col1") column1: Double,
                        @ColumnName("col4") column4: String)

  case class TestClass5(@CompoundKey("test", "1") @ColumnName("col3") column3: String,
                        @ColumnName("col1") column1: Double,
                        @ColumnName("col4") column4: String)

  case class TestOrder(col1: String, col2: Boolean, col3: Int, col4: Double)

  case class TestOrder2(@ColumnName("col1") col_1: String,
                        @ColumnName("col2") col_2: Boolean,
                        @ColumnName("col3") col_3: Int,
                        @ColumnName("col4") col_4: Double)

  case class TestException(col1: String, col2: Boolean, col3: Int, col4: Option[Double])

  case class TestException2(@ColumnName("col1") col_1: String,
                            @ColumnName("col2") col_2: Boolean,
                            @ColumnName("col3") col_3: Int,
                            @ColumnName("col4") col_4: Option[Double])

  case class TestException3(@ColumnName("col1") col_1: String,
                            @ColumnName("col2") col_2: Boolean,
                            @ColumnName("col3") col_3: Int,
                            @ColumnName("col4") col_4: Double)

  case class TestComplex(@CompoundKey("test", "1") @ColumnName("col1") col_1: String,
                         @ColumnName("col2") col_2: Boolean,
                         @ColumnName("col3") col_3: Int,
                         @ColumnName("col4") col_4: Double)

  case class TestComplex2(@CompoundKey("test", "1") @ColumnName("col1") col_1: String,
                         @ColumnName("col2") col_2: Boolean,
                         @ColumnName("col3") col_3: Int,
                         @ColumnName("col4") col_4: Double,
                         @Compress() @ColumnName("col5") col_5: Seq[String])


}

