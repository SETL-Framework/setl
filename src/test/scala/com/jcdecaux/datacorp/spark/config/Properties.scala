package com.jcdecaux.datacorp.spark.config

import com.typesafe.config.Config

object Properties extends ConfigLoader {

  val excelConfig: Config = this.getConfig("test.excel")
  val cassandraConfig: Config = this.getConfig("test.cassandra")

  val csvConfig: Config = this.getConfig("test.csv")
  val parquetConfig: Config = this.getConfig("test.parquet")

  val jsonConfig: Config = this.getConfig("test.json")

  val excelConfigConnector: Config = this.getConfig("connector.excel")
  val cassandraConfigConnector: Config = this.getConfig("connector.cassandra")
  val csvConfigConnector: Config = this.getConfig("connector.csv")
  val parquetConfigConnector: Config = this.getConfig("connector.parquet")
  val dynamoDbConfigConnector: Config = this.getConfig("connector.dynamo")

  val excelConfigConnectorBuilder: Config = this.getConfig("connectorBuilder.excel")
  val cassandraConfigConnectorBuilder: Config = this.getConfig("connectorBuilder.cassandra")
  val csvConfigConnectorBuilder: Config = this.getConfig("connectorBuilder.csv")
  val wrongCsvConfigConnectorBuilder: Config = this.getConfig("connectorBuilder.wrong_csv")
  val wrongCsvConfigConnectorBuilder2: Config = this.getConfig("connectorBuilder.wrong_csv2")
  val parquetConfigConnectorBuilder: Config = this.getConfig("connectorBuilder.parquet")


  val excelConfigRepoBuilder: Config = this.getConfig("repoBuilder.excel")
  val cassandraConfigRepoBuilder: Config = this.getConfig("repoBuilder.cassandra")
  val csvConfigRepoBuilder: Config = this.getConfig("repoBuilder.csv")
  val parquetConfigRepoBuilder: Config = this.getConfig("repoBuilder.parquet")

}
