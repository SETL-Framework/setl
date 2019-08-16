package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.enums.AppEnv
import com.jcdecaux.datacorp.spark.internal.Logging
import com.typesafe.config._


/**
  * ConfigLoader is used to load configurations by using typesafe.com's config library
  */
@InterfaceStability.Evolving
abstract class ConfigLoader extends Logging {
  val appName: String = "APP"
  val appEnvs: Array[String] = AppEnv.values().map(_.toString.toLowerCase())
  val fallBackConf: String = "application.conf"
  val clearCaches: Boolean = true

  def envNameVariable: String = System.getenv(s"${appName}_ENV")

  def envNameProperty: String = System.getProperty("app.environment")

  lazy val config: Config = {

    log.debug("Before execution of beforeAll")
    beforeAll()
    log.debug("After execution of beforeAll")

    if (clearCaches) {
      log.debug("Clear ConfigFactory caches")
      ConfigFactory.invalidateCaches()
    }

    if (appEnvs.contains(envNameVariable)) {
      log.debug(s"find $envNameVariable in system environmental variables")
      ConfigFactory.load(s"$envNameVariable.conf")
    } else if (appEnvs.contains(envNameProperty)) {
      log.debug(s"find $envNameProperty in jvm properties")
      ConfigFactory.load(s"$envNameProperty.conf")
    } else {
      log.debug(s"No env setting was found in neither system environmental variables nor jvm properties." +
        s"fallback configuration $fallBackConf will be loaded.")
      ConfigFactory.load(fallBackConf)
    }

  }

  def get(key: String): String = config.getString(key)

  def getObject(key: String): ConfigObject = config.getObject(key)

  def getConfig(key: String): Config = config.getConfig(key)

  /**
    * beforeAll will be called before loading the typesafe config file. User can override it with property settings
    */
  def beforeAll(): Unit = {}
}
