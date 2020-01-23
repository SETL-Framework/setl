package com.jcdecaux.setl.config

import org.scalatest.funsuite.AnyFunSuite

class DynamoDBConnectorConfSuite extends AnyFunSuite {

  val conf = new DynamoDBConnectorConf()

  test("Set DynamoDBConnectorConf") {
    assert(conf.get("table") === None)
    assert(conf.get("readPartitions") === None)
    conf.setTable("realTable")
    conf.setReadPartitions("realReadPartitions")

    assert(conf.get("table").get === "realTable")
    assert(conf.get("readPartitions").get === "realReadPartitions")
  }

  test("Getters of DynamoDBConnectorConf") {
    assert(conf.getTable === Some("realTable"))
    assert(conf.getReadPartitions === Some("realReadPartitions"))
  }
}
