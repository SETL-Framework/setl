package com.jcdecaux.setl.storage.connector
import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.enums.Storage
import com.typesafe.config.Config

/**
 * ConnectorInterface provides the abstraction of a pluggable connector that could be used by [[com.jcdecaux.setl.storage.ConnectorBuilder]].
 * Users can implement their customized data source connector by extending this trait.
 */
trait ConnectorInterface extends Connector {

  override val storage: Storage = Storage.OTHER

  def setConf(conf: Conf): Unit

  def setConfig(config: Config): Unit

}
