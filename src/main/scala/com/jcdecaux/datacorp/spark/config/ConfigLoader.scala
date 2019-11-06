package com.jcdecaux.datacorp.spark.config

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging
import com.typesafe.config._


/**
  * <p>ConfigLoader loads configurations by using typesafe.com's config library.</p>
  *
  * <p>You should set the application environment, then ConfigLoad will read the configuration
  * file <code>[app_env].conf</code> in your resources directory. </p>
  *
  * <p>Multiple solutions are possible to configure application environment:<br>
  * <ul>
  * <li>
  * Otherwise you can set the JVM property `app.environment`. For example, by adding `-Dapp.environment=local` in your command,
  * ConfigLoader will read the conf file `local.conf`
  * </li>
  * <li>
  * You can also create `app.environment` in the default configuration file (example: application.conf,
  *   application.properties)
  * </li>
  *
  * <li>The last solution is to overwrite directly the value of `confPath` with the name of you configuration file.
  * Then ConfigLoader will read directly the given file
  * </li>
  * </ul>
  *
  * <p>The priority of the above solution is:</p>
  * {{{
  *   configPath >> setAppEnv >> JVM property >> `app.environment` in default config file
  * }}}
  *
  * <p>If none of the above parameters are set, ConfigLoader will try to read its fallback configuration file</p>
  *
  */
@InterfaceStability.Evolving
abstract class ConfigLoader extends Logging {

  val appName: String = "APP"
  protected var applicationEnvironment: Option[String] = None
  val clearCaches: Boolean = true
  val configPath: Option[String] = None

  /**
    * Default configuration
    */
  private[this] val defaultConfig: Option[Config] = try {
    Option(ConfigFactory.load())
  } catch {
    case a: ConfigException.UnresolvedSubstitution =>
      log.warn(a.toString)
      None
    case e: Throwable => throw e
  }

  private[this] val defaultConfigAppEnvironment: Option[String] = defaultConfig match {
    case Some(config) =>
      try {
        Option(config.getString("app.environment"))
      } catch {
        case _: ConfigException.Missing =>
          log.debug("No app.environment was found in the default config file")
          None
        case e: Throwable => throw e
      }

    case _ => None
  }

  def appEnv: String = applicationEnvironment.get.toLowerCase()

  def getConfigPath: String = {
    val cf = configPath.getOrElse(s"$appEnv.conf")
    log.debug(s"Load config file: $cf")
    cf
  }

  private[this] def invalidateCaches(): Unit = {
    log.debug("Clear ConfigFactory caches")
    ConfigFactory.invalidateCaches()
  }

  private[this] def loadConfig(): Config = applicationEnvironment match {
    case Some(_) => ConfigFactory.load(getConfigPath)
    case _ => ConfigFactory.load()
  }

  def getAppEnvFromJvmProperties: Option[String] = Option(System.getProperty("app.environment"))

  /**
    * Update application environment (default LOCAL) by searching in system variables.
    *
    * @return true if applicationEnvironment has been updated, false otherwise
    */
  private[this] def updateAppEnv(): Unit = {
    if (getAppEnvFromJvmProperties.isDefined) {
      log.debug(s"Find AppEnv=$getAppEnvFromJvmProperties in jvm properties")
      applicationEnvironment = getAppEnvFromJvmProperties

    } else if (defaultConfigAppEnvironment.isDefined) {
      log.debug(s"Find key app.environment=$defaultConfigAppEnvironment in default config file")
      applicationEnvironment = defaultConfigAppEnvironment

    }
  }

  /**
    * Config loaded from filesystem
    */
  lazy val config: Config = {
    log.debug("Before execution of beforeAll")
    this.beforeAll()
    log.debug("After execution of beforeAll")
    if (clearCaches) this.invalidateCaches()

    // Try to update application environment if it's still undefined
    if (applicationEnvironment.isEmpty) updateAppEnv()

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
    var _applicationEnvironment: Option[String] = None
    val properties: ConcurrentHashMap[String, String] = new ConcurrentHashMap()

    def setAppEnv(env: String): this.type = {
      _applicationEnvironment = Option(env)
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

        applicationEnvironment = _applicationEnvironment

        override def beforeAll(): Unit = {
          import scala.collection.JavaConverters._
          properties.asScala.foreach {
            case (k, v) =>
              log.debug(s"Add property $k: $v")
              System.setProperty(k, v)
          }
        }
      }

      // we call config to activate the lazy val
      configLoader.config

      this
    }

    override def get(): ConfigLoader = configLoader
  }

  def builder(): Builder = new Builder
}
