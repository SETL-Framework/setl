package com.jcdecaux.datacorp.spark.config

import com.typesafe.config.Config

object Properties extends ConfigLoader {

  val excelConfig: Config = this.getObject("test.excel")
  val cassandraConfig: Config = this.getObject("test.cassandra")
  val csvConfig: Config = this.getObject("test.csv")
  val parquetConfig: Config = this.getObject("test.parquet")


}
