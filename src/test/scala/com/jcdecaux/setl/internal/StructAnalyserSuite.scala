package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.{ColumnName, CompoundKey, Compress}
import com.jcdecaux.setl.internal.TestClasses.TestStructAnalyser
import com.jcdecaux.setl.storage.{Compressor, XZCompressor}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class StructAnalyserSuite extends AnyFunSuite {

  val schema: StructType = StructAnalyser.analyseSchema[TestStructAnalyser]

  test("StructAnalyser should be able to handle @ColumnName") {
    val fields = schema.filter(_.metadata.contains(classOf[ColumnName].getCanonicalName))

    assert(fields.length === 1)
    assert(fields.head.name === "col1")
    assert(fields.head.metadata.getStringArray(classOf[ColumnName].getCanonicalName) === Array("alias1"))

  }

  test("StructAnalyser should be able to handle @CompoundKey") {
    val fields = schema.filter(_.metadata.contains(classOf[CompoundKey].getCanonicalName))

    assert(fields.length === 2)
    assert(fields.map(_.name) === Array("col2", "col22"))
    assert(fields.map(_.metadata.getStringArray(classOf[CompoundKey].getCanonicalName)).map(_ (0)) === List("test!@1", "test!@2"))
  }

  test("StructAnalyser should be able to handle @Compress") {
    val fields = schema.filter(_.metadata.contains(classOf[Compress].getCanonicalName))

    assert(fields.length === 2)
    assert(fields.map(_.name) === Array("col3", "col4"))

    assert(
      fields
        .find(_.name == "col3")
        .get.metadata
        .getStringArray(classOf[Compress].getCanonicalName)(0) === classOf[XZCompressor].getCanonicalName
    )

    assert(
      fields
        .find(_.name == "col4")
        .get.metadata
        .getStringArray(classOf[Compress].getCanonicalName)(0) === classOf[Compressor].getCanonicalName
    )
  }

  test("[SETL-34] StructAnalyser should handle multiple @CompoundKey annotations") {
    val structType = StructAnalyser.analyseSchema[TestClasses.MultipleCompoundKeyTest]
    structType.foreach { x =>
      println(s"name: ${x.name}, type: ${x.dataType}, meta: ${x.metadata}")
    }

    assert(structType.find(_.name == "col1").get.metadata.getStringArray(classOf[CompoundKey].getCanonicalName) === Array("sort!@1","part!@1"))
    assert(structType.find(_.name == "col2").get.metadata.getStringArray(classOf[CompoundKey].getCanonicalName) === Array("sort!@2"))
    assert(structType.find(_.name == "col3").get.metadata.getStringArray(classOf[CompoundKey].getCanonicalName) === Array("part!@2"))
  }


  test("StructAnalyser should be able to find columns with @CompoundKey") {
    val primaryColumns1 = StructAnalyser.findCompoundColumns[TestClasses.MultipleCompoundKeyTest]
    val primaryColumns2 = StructAnalyser.findCompoundColumns[TestClasses.MyObject]

    assert(primaryColumns1.length == 3)
    assert(primaryColumns1 === Array("col1", "col2", "COLUMN_3"))
    assert(primaryColumns2.isEmpty)
    assert(primaryColumns2 === Array())
  }

  test("[SETL-34] StructAnalyser should throw exception when there are more than one ColumnName annotation") {
    assertThrows[IllegalArgumentException](StructAnalyser.analyseSchema[TestClasses.WrongClass])
  }

}
