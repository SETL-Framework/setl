package com.jcdecaux.datacorp.spark.config

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.enums.AppEnv
import com.jcdecaux.datacorp.spark.internal.Logging
import com.typesafe.config._


/**
  * <p>ConfigLoader loads configurations by using typesafe.com's config library.</p>
  *
  * <p>You should set the application environment (for the available options, pleas refer to [[com.jcdecaux.datacorp.spark.enums.AppEnv]]). Then
  * ConfigLoad will read the configuration file <code>[app_env].conf</code> in your resources directory. </p>
  *
  * <p>Multiple solutions are possible to configure application environment:<br>
  * <ul>
  * <li>
  * You can overwrite the value of `appName` and set a system wide environmental variable corresponding to the appName value.
  * The default `appName` is APP so you can do:
  * {{{
  *     export APP_ENV=LOCAL
  * }}}
  *
  * to read the conf file `local.conf` <br>
  * If you set appName to MYAPP, then you need to do `export MYAPP_ENV=LOCAL`
  * </li>
  * <li>
  * Otherwise you can set the JVM property `app.environment`. For example, by adding `-Dapp.environment=local` in your command,
  * ConfigLoader will read the conf file `local.conf`
  * </li>
  * </ul>
  * </p>
  *
  * <p>Another solution is to overwrite directly the value of `confPath` with the name of you configuration file.
  * Then ConfigLoader will read directly the given file</p>
  *
  * <p>If none of the above parameters are set, ConfigLoader will try to read its fallback configuration file (by default
  * is `application.conf`)</p>
  *
  * The application environment is NOT case sensitive.
  */
@InterfaceStability.Evolving
abstract class ConfigLoader extends Logging {
  val appName: String = "APP"
  var appEnv: String = "local"
  val availableAppEnvs: Array[String] = AppEnv.values().map(_.toString.toLowerCase())
  val fallBackConf: String = "application.conf"
  val clearCaches: Boolean = true
  val configPath: Option[String] = None

  @throws[IllegalArgumentException]
  private[this] def validate(): Unit = {
    if (envNameVariable != "") {
      require(availableAppEnvs.contains(envNameVariable), s"Invalid environment ${envNameVariable} in system environmental variables")
    } else if (envNameProperty != "") {
      require(availableAppEnvs.contains(envNameProperty), s"Invalid environment ${envNameProperty} in JVM properties")
    }

  }

  private[this] def envNameVariable: String = {
    if (System.getenv(s"${appName}_ENV") == null) {
      ""
    } else {
      System.getenv(s"${appName}_ENV").toLowerCase()
    }
  }

  private[this] def envNameProperty: String = {
    if (System.getProperty("app.environment") == null) {
      ""
    } else {
      System.getProperty("app.environment").toLowerCase()
    }
  }

  private[this] def invalidateCaches(): Unit = {
    log.debug("Clear ConfigFactory caches")
    ConfigFactory.invalidateCaches()
  }

  private[this] def loadConfig(): Config = {

    configPath match {
      case Some(path) =>
        log.debug(s"Configuration path is defined ($path).")
        updateAppEnv()
        ConfigFactory.load(path)

      case _ =>
        log.debug("No defined configuration path. Fetch from environmental variables.")
        if (updateAppEnv()) {
          ConfigFactory.load(s"$appEnv.conf")
        } else {
          ConfigFactory.load(fallBackConf)
        }
    }
  }

  /**
    * Update application environment (default LOCAL) by searching in system variables.
    *
    * @return true if appEnv has been updated, false otherwise
    */
  private[this] def updateAppEnv(): Boolean = {
    if (availableAppEnvs.contains(envNameVariable)) {
      log.debug(s"Find AppEnv ($envNameVariable) in system environmental variables")
      appEnv = envNameVariable
      true
    } else if (availableAppEnvs.contains(envNameProperty)) {
      log.debug(s"Find AppEnv ($envNameProperty) in jvm properties")
      appEnv = envNameProperty
      true
    } else {
      log.debug(s"No AppEnv was found in neither system environmental variables nor JVM properties.")
      false
    }
  }

  /**
    * Config loaded from filesystem
    */
  lazy val config: Config = {
    log.debug("Before execution of beforeAll")
    this.beforeAll()
    log.debug("After execution of beforeAll")
    this.validate()
    if (clearCaches) this.invalidateCaches()
    loadConfig()
  }

  /**
    * Get the value of a key
    *
    * @param key path expression
    * @return the string value at the requested path
    * @throws ConfigException.Missing   if value is absent or null
    * @throws ConfigException.WrongType if value is not convertible to an Enum
    */
  def get(key: String): String = config.getString(key)

  /**
    * Get the value of a key
    *
    * @param key path expression
    * @return Option[String] if the key exists, None otherwise
    */
  def getOption(key: String): Option[String] = {
    if (has(key)) {
      Option(get(key))
    } else {
      None
    }
  }

  /**
    * Get the value of a key
    *
    * @param key path expression
    * @return the Array[String] value at the requested path
    */
  def getArray(key: String): Array[String] = {
    import scala.collection.JavaConverters._
    config.getStringList(key).asScala.toArray
  }

  /**
    * Check if a key is presented in a config
    *
    * @param key path expression
    * @return true if the config contains the key, otherwise false
    */
  def has(key: String): Boolean = config.hasPath(key)

  /**
    * Get the value of a key
    *
    * @param key path expression
    * @return the ConfigObject value at the requested path
    */
  def getObject(key: String): ConfigObject = config.getObject(key)

  /**
    * Get the value of a key
    *
    * @param key path expression
    * @return the nested Config value at the requested path
    */
  def getConfig(key: String): Config = config.getConfig(key)

  /**
    * BeforeAll will be called before loading the typesafe config file. User can override it with property settings
    */
  def beforeAll(): Unit = {}
}

object ConfigLoader {

  class Builder extends com.jcdecaux.datacorp.spark.Builder[ConfigLoader] {

    var configLoader: ConfigLoader = _
    var _appName: String = "APP"
    var _configPath: Option[String] = None
    val properties: ConcurrentHashMap[String, String] = new ConcurrentHashMap()

    def setAppEnv(env: String): this.type = {
      properties.put("app.environment", env)
      this
    }

    def setProperty(key: String, value: String): this.type = {
      properties.put(key, value)
      this
    }

    def setAppName(name: String): this.type = {
      _appName = name
      this
    }

    def setConfigPath(path: String): this.type = {
      _configPath = Option(path)
      this
    }

    /**
      * Build an object
      *
      * @return
      */
    override def build(): Builder.this.type = {

      configLoader = new ConfigLoader() {

        override val appName: String = _appName

        override val configPath: Option[String] = _configPath

        override def beforeAll(): Unit = {
          import scala.collection.JavaConverters._
          properties.asScala.foreach {
            case (k, v) =>
              log.debug(s"Add property $k: $v")
              System.setProperty(k, v)
          }
        }
      }

      this
    }

    override def get(): ConfigLoader = configLoader
  }

  def builder(): Builder = new Builder
}
