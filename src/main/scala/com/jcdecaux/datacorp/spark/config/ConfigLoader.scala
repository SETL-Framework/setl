package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.enums.AppEnv
import com.typesafe.config._


/**
  * ConfigLoader is used to load configurations by using typesafe.com's config library
  */
@InterfaceStability.Evolving
abstract class ConfigLoader {
  val appName: String = "APP"
  val appEnvs: Array[String] = AppEnv.values().map(_.toString.toLowerCase())
  val fallBackConf: String = "application.conf"

  def envNameVariable: String = System.getenv(s"${appName}_ENV")

  def envNameProperty: String = System.getProperty("app.environment")

  lazy val config: Config = {

    beforeAll()

    if (appEnvs.contains(envNameVariable)) {
      ConfigFactory.load(s"${envNameVariable}.conf")
    } else if (appEnvs.contains(envNameProperty)) {
      ConfigFactory.load(s"${envNameProperty}.conf")
    } else {
      ConfigFactory.load()
    }.withFallback(ConfigFactory.load(fallBackConf))

  }

  def get(key: String): String = config.getString(key)

  def getObject(key: String): ConfigObject = config.getObject(key)

  def getConfig(key: String): Config = config.getConfig(key)

  /**
    * beforeAll will be called before loading the typesafe config file. User can override it with property settings
    */
  def beforeAll(): Unit = {}
}
