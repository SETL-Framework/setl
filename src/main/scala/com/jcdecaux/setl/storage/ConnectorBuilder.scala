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

/**
 * ConnectorBuilder will build a [[com.jcdecaux.setl.storage.connector.Connector]] object with the given
 * configuration.
 *
 * @param config either a [[com.typesafe.config.Config]] or a [[com.jcdecaux.setl.config.Conf]] object
 */
@InterfaceStability.Evolving
class ConnectorBuilder(val config: Either[Config, Conf]) extends Builder[Connector] {

  private[setl] def this(config: Option[Config], conf: Option[Conf]) = this(
    (config, conf) match {
      case (Some(c), None) => Left(c)
      case (None, Some(c)) => Right(c)
      case (_, _) => throw new IllegalArgumentException("Can't build connector with redundant configurations")
    }
  )

  /**
   * ConnectorBuilder will build a [[com.jcdecaux.setl.storage.connector.Connector]] object with the given
   * configuration.
   *
   * @param config a [[com.typesafe.config.Config]] object
   */
  def this(config: Config) = this(Left(config))

  /**
   * ConnectorBuilder will build a [[com.jcdecaux.setl.storage.connector.Connector]] object with the given
   * configuration.
   *
   * @param conf a [[com.jcdecaux.setl.config.Conf]] object
   */
  def this(conf: Conf) = this(Right(conf))

  private[this] var connector: Connector = _

  /**
   * Build a connector
   */
  override def build(): ConnectorBuilder.this.type = {
    connector = buildConnector(config)
    this
  }

  /**
   * Build a connector from either a [[com.typesafe.config.Config]] or a [[com.jcdecaux.setl.config.Conf]] object.
   *
   * the `config` object must have a key `storage` and the parameters corresponding to the storage
   *
   * @param config either a [[com.typesafe.config.Config]] or a [[com.jcdecaux.setl.config.Conf]] object
   * @return [[com.jcdecaux.setl.storage.connector.Connector]] a connector object
   */
  private[this] def buildConnector(config: Either[Config, Conf]): Connector = {
    val storage = config match {
      case Left(c) =>
        TypesafeConfigUtils.getAs[Storage](c, ConnectorBuilder.STORAGE)
      case Right(c) =>
        c.getAs[Storage](ConnectorBuilder.STORAGE)
    }

    require(storage.nonEmpty, "Storage type is not defined")


    if (storage.get != Storage.OTHER) {
      val argClass = if (config.isLeft) {
        classOf[Config]
      } else {
        classOf[Conf]
      }

      log.debug(s"Build ${storage.get} connector with ${argClass.getCanonicalName}")

      if (classOf[ConnectorInterface].isAssignableFrom(Class.forName(storage.get.connectorName()))) {
        log.debug("Detect V2 connector")
        instantiateConnectorInterface(storage.get.connectorName(), config)
      } else {
        val constructor = connectorConstructorOf(storage.get, argClass)
        constructor.newInstance(
          config match {
            case Left(c) => c
            case Right(c) => c
          }
        )
      }


    } else {

      val connectorName = config match {
        case Left(c) =>
          TypesafeConfigUtils.getAs[String](c, ConnectorBuilder.CLASS)
        case Right(c) =>
          c.getAs[String](ConnectorBuilder.CLASS)
      }

      require(connectorName.nonEmpty, s"A class reference of the custom connector must be provided in the '${ConnectorBuilder.CLASS}' field'")

      instantiateConnectorInterface(connectorName.get, config)
    }

  }

  /**
   * Instantiate a ConnectorInterface
   * @param cls string of connector interface name
   * @param config configuration
   * @return
   */
  private[this] def instantiateConnectorInterface(cls: String, config: Either[Config, Conf]): ConnectorInterface = {
    val _cls = Class.forName(cls)
    require(classOf[ConnectorInterface].isAssignableFrom(_cls), s"The class $cls is not an implementation of ConnectorInterface")
    val cst = _cls.getDeclaredConstructor()
    cst.setAccessible(true)
    val connectorInterface = cst.asInstanceOf[Constructor[ConnectorInterface]].newInstance()

    config match {
      case Left(c) => connectorInterface.setConfig(c)
      case Right(c) => connectorInterface.setConf(c)
    }

    connectorInterface
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

  override def get(): Connector = connector
}

object ConnectorBuilder {
  val STORAGE: String = "storage"
  val CLASS: String = "class"
}