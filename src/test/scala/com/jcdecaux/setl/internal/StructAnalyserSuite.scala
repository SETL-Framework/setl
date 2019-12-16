package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.{ColumnName, CompoundKey, Compress}
import com.jcdecaux.setl.internal.TestClasses.TestStructAnalyser
import com.jcdecaux.setl.storage.{Compressor, XZCompressor}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class StructAnalyserSuite extends AnyFunSuite {

  val schema: StructType = StructAnalyser.analyseSchema[TestStructAnalyser]

  test("StructAnalyser should be able to handle @ColumnName") {
    val fields = schema.filter(_.metadata.contains(ColumnName.toString()))

    assert(fields.length === 1)
    assert(fields.head.name === "col1")
    assert(fields.head.metadata.getStringArray(ColumnName.toString()) === Array("alias1"))

  }

  test("StructAnalyser should be able to handle @CompoundKey") {
    val fields = schema.filter(_.metadata.contains(CompoundKey.toString()))

    assert(fields.length === 2)
    assert(fields.map(_.name) === Array("col2", "col22"))
    assert(fields.map(_.metadata.getStringArray(CompoundKey.toString())).map(_ (0)) === List("test", "test"))
    assert(fields.map(_.metadata.getStringArray(CompoundKey.toString())).map(_ (1)) === List("1", "2"))

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

}
