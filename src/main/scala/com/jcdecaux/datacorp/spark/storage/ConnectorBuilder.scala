package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.Builder
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.storage.connector._
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
  * ConnectorBuilder will build a [[com.jcdecaux.datacorp.spark.storage.connector.Connector]] object with the given
  * configuration.
  *
  * @param spark  spark session
  * @param config optional, a [[com.typesafe.config.Config]] object
  * @param conf   optional, a [[com.jcdecaux.datacorp.spark.config.Conf]] object
  */
class ConnectorBuilder(val spark: SparkSession, val config: Option[Config], val conf: Option[Conf]) extends Builder[Connector] {

  def this(spark: SparkSession, config: Config) = this(spark, Some(config), None)

  def this(spark: SparkSession, conf: Conf) = this(spark, None, Some(conf))

  private[this] var connector: Connector = _

  /**
    * Build a connector
    */
  override def build(): ConnectorBuilder.this.type = {

    connector = (config, conf) match {
      case (Some(c), None) => buildConnectorWithConfig(c)
      case (None, Some(c)) => buildConnectorWithConf(c)
      case (_, _) => throw new IllegalArgumentException("Can't build connector with redundant configurations")
    }

    this
  }

  /**
    * Build a connector from a [[Conf]] object.
    *
    * the `Conf` object must have a key `storage` and the parameters corresponding to the storage
    *
    * @param conf [[com.jcdecaux.datacorp.spark.config.Conf]] configuration
    * @return [[com.jcdecaux.datacorp.spark.storage.connector.Connector]] a connector object
    */
  private[this] def buildConnectorWithConf(conf: Conf): Connector = {
    conf.getAs[Storage]("storage") match {

      case Some(Storage.CASSANDRA) =>
        log.debug("Find cassandra storage")
        new CassandraConnector(spark, conf)

      case Some(Storage.EXCEL) =>
        log.debug("Find excel storage")

        if (conf.getAs[Boolean]("inferSchema").get & conf.getAs[String]("schema").isEmpty) {
          log.warn("Excel connect may not behave as expected when parsing/saving Integers. " +
            "It's recommended to define a schema instead of infer one")
        }
        new ExcelConnector(spark, conf)

      case Some(Storage.CSV) =>
        log.debug("Find csv storage")
        new CSVConnector(spark, conf)

      case Some(Storage.PARQUET) =>
        log.debug("Find parquet storage")
        new ParquetConnector(spark, conf)

      case Some(Storage.DYNAMODB) =>
        log.debug("Find dynamodb storage")
        new DynamoDBConnector(spark, conf)

      case Some(Storage.JSON) =>
        log.debug("Find dynamodb storage")
        new JSONConnector(spark, conf)

      case _ =>
        throw new UnknownException.Storage("Unknown storage type")
    }
  }

  /**
    * Build a connector from a [[Config]] object.
    *
    * the `Config` object must have a key `storage` and the parameters corresponding to the storage
    *
    * @param config [[com.typesafe.config.Config]]
    * @return [[com.jcdecaux.datacorp.spark.storage.connector.Connector]]
    */
  private[this] def buildConnectorWithConfig(config: Config): Connector = {
    TypesafeConfigUtils.getAs[Storage](config, "storage") match {

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

      case Some(Storage.JSON) =>
        log.debug("Find dynamodb storage")
        new JSONConnector(spark, config)

      case _ =>
        throw new UnknownException.Storage("Unknown storage type")
    }
  }

  override def get(): Connector = connector
}
