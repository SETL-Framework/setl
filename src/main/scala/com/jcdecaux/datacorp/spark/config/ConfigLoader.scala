package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.typesafe.config._


/**
  * ConfigLoader is used to load configurations by using typesafe.com's config library
  */
@InterfaceStability.Evolving
class ConfigLoader {

  private[this] val fallBackConf: String = "application.conf"

  val config: Config =
    if (Seq("dev", "lab", "prod").contains(System.getenv("APP_ENV"))) {
      ConfigFactory.load(System.getenv("APP_ENV") + ".conf")
    } else if (Seq("dev", "lab", "prod").contains(System.getProperty("app.environment"))) {
      ConfigFactory.load(System.getProperty("app.environment") + ".conf")
    } else {
      ConfigFactory.load()
    }.withFallback(ConfigFactory.load(fallBackConf))

  def get(key: String): String = config.getString(key)

  def getObject(key: String): ConfigObject = config.getObject(key)

  def getConfig(key: String): Config = config.getConfig(key)
}
