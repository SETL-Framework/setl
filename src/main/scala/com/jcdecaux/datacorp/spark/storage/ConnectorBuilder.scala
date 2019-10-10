package com.jcdecaux.datacorp.spark.storage

import java.lang.reflect.Constructor

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
    * Instantiate a constructor of connector according to the storage type and constructor's arguments
    *
    * @param storage         storage enum
    * @param constructorArgs type of constructor arguments
    * @return
    */
  @throws[NoSuchMethodException]
  private[this] def connectorConstructorOf(storage: Storage, constructorArgs: Class[_]*): Constructor[Connector] = {
    storage match {
      case Storage.OTHER =>
        throw new UnknownException.Storage("Storage OTHER is not supported")
      case _ =>
        val cst = Class.forName(storage.connectorName())
          .getDeclaredConstructor(constructorArgs: _*)
        cst.setAccessible(true)
        cst.asInstanceOf[Constructor[Connector]]
    }
  }

  /**
    * Build a connector from a [[com.jcdecaux.datacorp.spark.config.Conf]] object.
    *
    * the `Conf` object must have a key `storage` and the parameters corresponding to the storage
    *
    * @param configuration [[com.jcdecaux.datacorp.spark.config.Conf]] configuration
    * @return [[com.jcdecaux.datacorp.spark.storage.connector.Connector]] a connector object
    */
  private[this] def buildConnectorWithConf(configuration: Conf): Connector = {
    configuration.getAs[Storage]("storage") match {
      case Some(s) =>
        val constructor = connectorConstructorOf(s, classOf[SparkSession], classOf[Conf])
        constructor.newInstance(spark, configuration)
      case _ => throw new UnknownException.Storage("Unknown storage type")
    }
  }

  /**
    * Build a connector from a [[com.typesafe.config.Config]] object.
    *
    * the `Config` object must have a key `storage` and the parameters corresponding to the storage
    *
    * @param configuration a [[com.typesafe.config.Config]] object
    * @return a [[com.jcdecaux.datacorp.spark.storage.connector.Connector]]
    */
  private[this] def buildConnectorWithConfig(configuration: Config): Connector = {

    TypesafeConfigUtils.getAs[Storage](configuration, "storage") match {
      case Some(s) =>
        val constructor = connectorConstructorOf(s, classOf[SparkSession], classOf[Config])
        constructor.newInstance(spark, configuration)
      case _ => throw new UnknownException.Storage("Unknown storage type")
    }
  }

  override def get(): Connector = connector
}
