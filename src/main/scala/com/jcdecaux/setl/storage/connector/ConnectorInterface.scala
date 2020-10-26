package com.jcdecaux.setl.storage.connector
import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.enums.Storage
import com.typesafe.config.Config

/**
 * ConnectorInterface provides the abstraction of a pluggable connector that could be used by [[com.jcdecaux.setl.storage.ConnectorBuilder]].
 * Users can implement their customized data source connector by extending this trait.
 */
trait ConnectorInterface extends Connector {

  /**
   * By default, the custom connector's storage type should be OTHER.
   */
  override val storage: Storage = Storage.OTHER

  /**
   * Configure the connector with the given [[Conf]]
   * @param conf an object of [[Conf]]
   */
  def setConf(conf: Conf): Unit

  /**
   * Configure the connector with the given [[Config]]
   * @param config an object of [[Config]]
   */
  def setConfig(config: Config): Unit = this.setConf(Conf.fromConfig(config))

}
