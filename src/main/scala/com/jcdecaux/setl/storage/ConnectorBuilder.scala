package com.jcdecaux.setl.storage

import java.lang.reflect.Constructor

import com.jcdecaux.setl.Builder
import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.exception.UnknownException
import com.jcdecaux.setl.storage.connector._
import com.jcdecaux.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * ConnectorBuilder will build a [[com.jcdecaux.setl.storage.connector.Connector]] object with the given
 * configuration.
 *
 * @param config optional, a [[com.typesafe.config.Config]] object
 * @param conf   optional, a [[com.jcdecaux.setl.config.Conf]] object
 */
@InterfaceStability.Evolving
class ConnectorBuilder(val config: Option[Config],
                       val conf: Option[Conf]) extends Builder[Connector] {

  def this(config: Config) = this(Some(config), None)

  def this(conf: Conf) = this(None, Some(conf))

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, config: Option[Config], conf: Option[Conf]) = this(config, conf)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, config: Config) = this(Some(config), None)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, conf: Conf) = this(None, Some(conf))

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
   * Instantiate a connector constructor according to the storage type and constructor's arguments
   *
   * @param storage         storage enum
   * @param constructorArgs type of constructor arguments
   * @return
   */
  @throws[NoSuchMethodException]
  private[this] def connectorConstructorOf(storage: Storage, constructorArgs: Class[_]*): Constructor[Connector] = {
    if (storage.connectorName() == null) {
      throw new UnknownException.Storage(s"Storage $storage is not supported")
    }
    val cst = Class.forName(storage.connectorName()).getDeclaredConstructor(constructorArgs: _*)
    cst.setAccessible(true)
    cst.asInstanceOf[Constructor[Connector]]
  }

  /**
   * Build a connector from a [[com.jcdecaux.setl.config.Conf]] object.
   *
   * the `Conf` object must have a key `storage` and the parameters corresponding to the storage
   *
   * @param configuration [[com.jcdecaux.setl.config.Conf]] configuration
   * @return [[com.jcdecaux.setl.storage.connector.Connector]] a connector object
   */
  private[this] def buildConnectorWithConf(configuration: Conf): Connector = {
    configuration.getAs[Storage]("storage") match {
      case Some(s) =>
        val constructor = connectorConstructorOf(s, classOf[Conf])
        constructor.newInstance(configuration)
      case _ => throw new UnknownException.Storage("Unknown storage type")
    }
  }

  /**
   * Build a connector from a [[com.typesafe.config.Config]] object.
   *
   * the `Config` object must have a key `storage` and the parameters corresponding to the storage
   *
   * @param configuration a [[com.typesafe.config.Config]] object
   * @return a [[com.jcdecaux.setl.storage.connector.Connector]]
   */
  private[this] def buildConnectorWithConfig(configuration: Config): Connector = {

    TypesafeConfigUtils.getAs[Storage](configuration, "storage") match {
      case Some(s) =>
        val constructor = connectorConstructorOf(s, classOf[Config])
        constructor.newInstance(configuration)
      case _ => throw new UnknownException.Storage("Unknown storage type")
    }
  }

  override def get(): Connector = connector
}
