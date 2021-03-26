package io.github.setl.config

import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class JDBCConnectorConfSuite extends AnyFunSuite {

  val conf = new JDBCConnectorConf()
  val url = "url"
  val dbTable = "dbtable"
  val user = "user"
  val password = "password"
  val numPartitions = "numPartitions"
  val partitionColumn = "partitionColumn"
  val lowerBound = "lowerBound"
  val upperBound = "upperBound"
  val fetchSize = "fetchsize"
  val batchSize = "batchsize"
  val truncate = "truncate"

  test("Set JDBCConnectorConf") {
    assert(conf.get(url) === None)
    conf.setUrl(url)
    assert(conf.get(url).get === url)

    assert(conf.get(dbTable) === None)
    conf.setDbTable(dbTable)
    assert(conf.get(dbTable).get === dbTable)

    assert(conf.get(user) === None)
    conf.setUser(user)
    assert(conf.get(user).get === user)

    assert(conf.get(password) === None)
    conf.setPassword(password)
    assert(conf.get(password).get === password)

    assert(conf.get("saveMode") === None)
    conf.setSaveMode("Overwrite")
    assert(conf.get("saveMode").get === "Overwrite")

    conf.setSaveMode(SaveMode.Append)
    assert(conf.get("saveMode").get === "Append")

    assert(conf.get(numPartitions) === None)
    conf.setNumPartitions(numPartitions)
    assert(conf.get(numPartitions).get === numPartitions)

    assert(conf.get(partitionColumn) === None)
    conf.setPartitionColumn(partitionColumn)
    assert(conf.get(partitionColumn).get === partitionColumn)

    assert(conf.get(lowerBound) === None)
    conf.setLowerBound(lowerBound)
    assert(conf.get(lowerBound).get === lowerBound)

    assert(conf.get(upperBound) === None)
    conf.setUpperBound(upperBound)
    assert(conf.get(upperBound).get === upperBound)

    assert(conf.get(fetchSize) === None)
    conf.setFetchSize(fetchSize)
    assert(conf.get(fetchSize).get === fetchSize)

    assert(conf.get(batchSize) === None)
    conf.setBatchSize(batchSize)
    assert(conf.get(batchSize).get === batchSize)

    assert(conf.get(truncate) === None)
    conf.setTruncate(truncate)
    assert(conf.get(truncate).get === truncate)
  }

  test("Getters of JDBCConnectorConf") {
    assert(conf.getUrl === Some(url))
    assert(conf.getDbTable === Some(dbTable))
    assert(conf.getUser === Some(user))
    assert(conf.getPassword === Some(password))
    assert(conf.getSaveMode === Some("Append"))
    assert(conf.getNumPartitions === Some(numPartitions))
    assert(conf.getPartitionColumn === Some(partitionColumn))
    assert(conf.getLowerBound === Some(lowerBound))
    assert(conf.getUpperBound === Some(upperBound))
    assert(conf.getFetchSize === Some(fetchSize))
    assert(conf.getBatchSize === Some(batchSize))
    assert(conf.getTruncate === Some(truncate))
  }
}
