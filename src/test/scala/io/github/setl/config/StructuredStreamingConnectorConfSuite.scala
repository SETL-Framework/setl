package io.github.setl.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StructuredStreamingConnectorConfSuite extends AnyFunSuite with Matchers {


  test("StructureStreamingConnectorConf exceptions") {
    val conf = new StructuredStreamingConnectorConf()
    assertThrows[IllegalArgumentException](conf.getSchema)
    assertThrows[IllegalArgumentException](conf.getFormat)
    assertThrows[IllegalArgumentException](conf.getPath)
    assertThrows[IllegalArgumentException](conf.getOutputMode)

    assert(conf.getReaderConf === Map())
    assert(conf.getReaderConf === Map())

  }

  test("StructureStreamingConnectorConf getFormat should always output lowercase") {
    val conf = new StructuredStreamingConnectorConf()
    conf.setFormat("PARQUET")
    assert(conf.getFormat === "parquet")
  }

  test("Setters and getters") {
    val conf = new StructuredStreamingConnectorConf()
    conf.setFormat("parquet")
    conf.setPath("test/path")
    conf.setOutputMode("outputmode")
    conf.setSchema("schema")

    conf.set("header", "header1")

    assert(conf.getFormat === "parquet")
    assert(conf.getPath === "test/path")
    assert(conf.getOutputMode === "outputmode")
    assert(conf.getSchema === "schema")
    assert(conf.get("header") === Option("header1"))

    conf.getReaderConf.keys should contain theSameElementsAs Array("header", "path")
    conf.getWriterConf.keys should contain theSameElementsAs Array("header", "path")
  }

  test("Construction from Map") {
    val map = Map(
      "format" -> "PARQUET",
      "outputMode" -> "append",
      "checkpointLocation" -> "test_checkpoint",
      "path" -> "test_path"
    )

    val conf = StructuredStreamingConnectorConf.fromMap(map)

    assert(conf.getFormat === "parquet")
    assert(conf.getPath === "test_path")
    assert(conf.getOutputMode === "append")
    assert(conf.get("checkpointLocation") === Some("test_checkpoint"))
    assert(conf.get("none") === None)
  }


}
