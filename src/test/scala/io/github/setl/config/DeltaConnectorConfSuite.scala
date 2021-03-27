package io.github.setl.config

import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class DeltaConnectorConfSuite extends AnyFunSuite {

  val conf = new DeltaConnectorConf()

  test("Set DeltaConnectorConf") {
    assert(conf.get("path") === None)
    assert(conf.get("saveMode") === None)
    conf.setPath("./path")
    conf.setSaveMode(SaveMode.Overwrite)

    assert(conf.get("path").get === "./path")
    assert(conf.get("saveMode").get === "Overwrite")
  }

  test("Getters of DynamoDBConnectorConf") {
    assert(conf.getPath === "./path")
    assert(conf.getSaveMode === SaveMode.Overwrite)
  }
}
