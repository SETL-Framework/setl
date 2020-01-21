package com.jcdecaux.setl.config

import com.typesafe.config.Config

object Properties {

  //  override def beforeAll(): Unit = {
  //    System.setProperty("myvalue", "test-my-value")
  //  }
  //
  val cl: ConfigLoader = ConfigLoader
    .builder()
    .setProperty("myvalue", "test-my-value")
    .setConfigPath("application.conf").getOrCreate()

  val excelConfig: Config = cl.getConfig("test.excel")
  val cassandraConfig: Config = cl.getConfig("test.cassandra")

  val csvConfig: Config = cl.getConfig("test.csv")
  val parquetConfig: Config = cl.getConfig("test.parquet")

  val jsonConfig: Config = cl.getConfig("test.json")

  val jdbcConfig: Config = cl.getConfig("psql.test")

  val excelConfigConnector: Config = cl.getConfig("connector.excel")
  val cassandraConfigConnector: Config = cl.getConfig("connector.cassandra")
  val csvConfigConnector: Config = cl.getConfig("connector.csv")
  val parquetConfigConnector: Config = cl.getConfig("connector.parquet")
  val dynamoDbConfigConnector: Config = cl.getConfig("connector.dynamo")

  val excelConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.excel")
  val cassandraConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.cassandra")
  val csvConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.csv")
  val jsonConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.json")


  val wrongCsvConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.wrong_csv")
  val wrongCsvConfigConnectorBuilder2: Config = cl.getConfig("connectorBuilder.wrong_csv2")
  val parquetConfigConnectorBuilder: Config = cl.getConfig("connectorBuilder.parquet")


  val excelConfigRepoBuilder: Config = cl.getConfig("repoBuilder.excel")
  val cassandraConfigRepoBuilder: Config = cl.getConfig("repoBuilder.cassandra")
  val csvConfigRepoBuilder: Config = cl.getConfig("repoBuilder.csv")
  val parquetConfigRepoBuilder: Config = cl.getConfig("repoBuilder.parquet")

}
