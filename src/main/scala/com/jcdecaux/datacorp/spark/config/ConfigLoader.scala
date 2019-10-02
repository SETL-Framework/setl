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

  @throws[IllegalArgumentException]
  private[this] def validate(): Unit = {
    if (envNameVariable != "") {
      require(availableAppEnvs.contains(envNameVariable), s"Invalid environment ${envNameVariable} in system environmental variables")
    } else if (envNameProperty != "") {
      require(availableAppEnvs.contains(envNameProperty), s"Invalid environment ${envNameProperty} in JVM properties")
    }

  }

  def envNameVariable: String = {
    if (System.getenv(s"${appName}_ENV") == null) {
      ""
    } else {
      System.getenv(s"${appName}_ENV").toLowerCase()
    }
  }

  def envNameProperty: String = {
    if (System.getProperty("app.environment") == null) {
      ""
    } else {
      System.getProperty("app.environment").toLowerCase()
    }
  }

  lazy val config: Config = {

    log.debug("Before execution of beforeAll")
    beforeAll()
    log.debug("After execution of beforeAll")

    validate()

    if (clearCaches) {
      log.debug("Clear ConfigFactory caches")
      ConfigFactory.invalidateCaches()
    }

    if (availableAppEnvs.contains(envNameVariable)) {
      log.debug(s"find $envNameVariable in system environmental variables")
      appEnv = envNameVariable
      ConfigFactory.load(s"$envNameVariable.conf")
    } else if (availableAppEnvs.contains(envNameProperty)) {
      log.debug(s"find $envNameProperty in jvm properties")
      appEnv = envNameProperty
      ConfigFactory.load(s"$envNameProperty.conf")
    } else {
      log.debug(s"No app ENV setting was found in neither system environmental variables nor JVM properties. " +
        s"configuration $fallBackConf will be loaded.")
      ConfigFactory.load(fallBackConf)
    }
  }

  def get(key: String): String = config.getString(key)

  def getOption(key: String): Option[String] = {
    if (has(key)) {
      Option(get(key))
    } else {
      None
    }
  }

  def getArray(key: String): Array[String] = {
    import scala.collection.JavaConverters._
    config.getStringList(key).asScala.toArray
  }

  def has(key: String): Boolean = config.hasPath(key)

  def getObject(key: String): ConfigObject = config.getObject(key)

  def getConfig(key: String): Config = config.getConfig(key)

  /**
    * beforeAll will be called before loading the typesafe config file. User can override it with property settings
    */
  def beforeAll(): Unit = {}
}

object ConfigLoader {

  class Builder extends com.jcdecaux.datacorp.spark.Builder[ConfigLoader] {

    var configLoader: ConfigLoader = _
    var _appName: String = "APP"
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

    /**
      * Build an object
      *
      * @return
      */
    override def build(): Builder.this.type = {

      configLoader = new ConfigLoader() {

        override val appName: String = _appName

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
