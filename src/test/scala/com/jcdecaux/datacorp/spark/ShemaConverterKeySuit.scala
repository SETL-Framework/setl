//package com.jcdecaux.datacorp.spark
//
//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import com.jcdecaux.datacorp.spark.annotations.CombinedKey
//import com.jcdecaux.datacorp.spark.internal.SchemaConverterKey
//import org.apache.spark.sql.{Dataset, SparkSession}
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//class ShemaConverterKeySuit extends FunSuite with DataFrameSuiteBase with BeforeAndAfterAll {
//
//  import spark.implicits._
//  var ds: Dataset[Reach] = _
//
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    ds = spark.createDataset(Seq(Reach("a", "A"), Reach("b", "B")))
//  }
//
//  /**
//    *
//    */
//  test("SchemaConverterKey ToDF") {
//
//    val df = SchemaConverterKey.toDF(ds)
//    df.show
//    assertTrue(df.columns.contains("key"))
//
//  }
//
//  /**
//    *
//    */
//  test("SchemaConverterKey fromDF[A]") {
//
//    val df = SchemaConverterKey.toDF(ds)
//    df.show
//
//    val dss = SchemaConverterKey.fromDF[Reach](df)
//    dss.show
//
//    assertTrue(!dss.columns.contains("key"))
//
//  }
//}
//
//case class Reach(@CombinedKey("1") column1: String, @CombinedKey("2") column2: String)
