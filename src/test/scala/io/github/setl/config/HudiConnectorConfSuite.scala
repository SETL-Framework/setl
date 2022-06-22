package io.github.setl.config

import io.github.setl.exception.ConfException
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SaveMode

class HudiConnectorConfSuite  extends AnyFunSuite {
  val conf = new HudiConnectorConf

  test("Get/Set HudiConnectorConf") {
    assert(conf.get("saveMode") === None)
    conf.setSaveMode("Append")
    assert(conf.getSaveMode === SaveMode.Append)
    conf.setSaveMode("Overwrite")
    assert(conf.getSaveMode === SaveMode.Overwrite)
    conf.setSaveMode(SaveMode.Overwrite)
    assert(conf.getSaveMode === SaveMode.Overwrite)

    assert(conf.get("path") === None)
    assertThrows[ConfException](conf.getPath)

    conf.setPath("path")
    assert(conf.getPath === "path")
  }

  test("Init HudiConnectorConf from options") {
    val options : Map[String, String] = Map(
      "path" -> "path",
      "saveMode" -> "Append",
      "hoodie.table.name" -> "test_object",
      "hoodie.datasource.write.recordkey.field" -> "col1",
      "hoodie.datasource.write.precombine.field" -> "col4",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ"
    )

    val confFromOpts: HudiConnectorConf = HudiConnectorConf.fromMap(options)
    assert(confFromOpts.getPath === "path")
    assert(confFromOpts.getSaveMode === SaveMode.Append)

    val readerOpts = confFromOpts.getReaderConf
    val writerOpts = confFromOpts.getWriterConf

    // Config should not contains path & save mode
    assert(!readerOpts.contains("path"))
    assert(!writerOpts.contains("path"))
    assert(!writerOpts.contains("saveMode"))
  }
}
