package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.factory.Builder
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ConnectorBuilder(val spark: SparkSession, val config: Config) extends Builder[Connector] with Logging {

  private var connector: Connector = _

  /**
    * Build an object
    *
    * @return
    */
  override def build(): ConnectorBuilder.this.type = {
    connector = TypesafeConfigUtils.getAs[Storage](config, "storage") match {

      case Some(Storage.CASSANDRA) =>
        log.debug("Find cassandra storage")
        new CassandraConnector(spark, config)

      case Some(Storage.EXCEL) =>
        log.debug("Find excel storage")
        new ExcelConnector(spark, config)

      case Some(Storage.CSV) =>
        log.debug("Find csv storage")
        new CSVConnector(spark, config)

      case Some(Storage.PARQUET) =>
        log.debug("Find parquet storage")
        new ParquetConnector(spark, config)

      case Some(Storage.DYNAMODB) =>
        log.debug("Find dynamodb storage")
        new DynamoDBConnector(spark, config)

      case _ =>
        throw new UnknownException.Storage("Unknown storage type")
    }

    this
  }

  override def get(): Connector = connector
}
