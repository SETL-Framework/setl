package com.jcdecaux.datacorp.spark.config

import com.typesafe.config.Config

object Properties extends ConfigLoader {

  val excelConfig: Config = this.getObject("test.excel")
  val cassandraConfig: Config = this.getObject("test.cassandra")

  val csvConfig: Config = this.getObject("test.csv")
  val parquetConfig: Config = this.getObject("test.parquet")

  val excelConfigConnector: Config = this.getObject("connector.excel")
  val cassandraConfigConnector: Config = this.getObject("connector.cassandra")
  val csvConfigConnector: Config = this.getObject("connector.csv")
  val parquetConfigConnector: Config = this.getObject("connector.parquet")
  val dynamoDbConfigConnector: Config = this.getObject("connector.dynamo")

  val excelConfigConnectorBuilder: Config = this.getObject("connectorBuilder.excel")
  val cassandraConfigConnectorBuilder: Config = this.getObject("connectorBuilder.cassandra")
  val csvConfigConnectorBuilder: Config = this.getObject("connectorBuilder.csv")
  val wrongCsvConfigConnectorBuilder: Config = this.getObject("connectorBuilder.wrong_csv")
  val wrongCsvConfigConnectorBuilder2: Config = this.getObject("connectorBuilder.wrong_csv2")
  val parquetConfigConnectorBuilder: Config = this.getObject("connectorBuilder.parquet")


  val excelConfigRepoBuilder: Config = this.getObject("repoBuilder.excel")
  val cassandraConfigRepoBuilder: Config = this.getObject("repoBuilder.cassandra")
  val csvConfigRepoBuilder: Config = this.getObject("repoBuilder.csv")
  val parquetConfigRepoBuilder: Config = this.getObject("repoBuilder.parquet")

}
