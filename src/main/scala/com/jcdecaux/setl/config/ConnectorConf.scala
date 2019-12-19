package com.jcdecaux.setl.config

abstract class ConnectorConf extends Conf {

  def getReaderConf: Map[String, String]

  def getWriterConf: Map[String, String]

}
