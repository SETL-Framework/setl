package com.jcdecaux.datacorp.spark

import com.jcdecaux.datacorp.spark.enums.AppEnv
import com.jcdecaux.datacorp.spark.transformation.Builder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Configure and build new sparkSession according to given usages
  *
  * <br>Usage:
  *
  * {{{
  *   // Auto-configure
  *   val spark: SparkSession = new SparkSessionBuilder("cassandra", "postgres").build().get()
  *
  *   // Build with your own SparkConf
  *   val spark: SparkSession = new SparkSessionBuilder().configure(yourSparkConf).build().get()
  * }}}
  *
  * @param usages usages of the sparkSession, could be a list of the following elements:
  *               <ul>
  *               <li>cassandra</li>
  *               <li>TODO</li>
  *               </ul>
  */
class SparkSessionBuilder(usages: String*) extends Builder[SparkSession] {

  var appName: String = "SparkApplication"
  var appEnv: AppEnv = AppEnv.DEV
  var cassandraHost: String = _
  var config: SparkConf = new SparkConf()
  var initialization: Boolean = true
  var sparkHost: String = "local[*]"
  private[spark] var spark: SparkSession = _

  /**
    * Automatic configuration according to the settings
    *
    * @return
    */
  def build(): this.type = {
    if (initialization) {
      log.debug("Initialize spark config")
      this.config = new SparkConf()
    }

    this.configureGeneralProperties()

    this.configureEnvironmentProperties()

    usages.toSet.foreach((usage: String) => {
      usage match {
        case "cassandra" =>
          if (cassandraHost != null) {
            this.config.set("spark.cassandra.connection.host", cassandraHost)
          } else {
            throw new NoSuchElementException("Cassandra host not set")
          }
        case "test" => log.warn("Testing usage")
        case _ => log.error(s"Skip unknown usage: $usage")
      }
    })

    log.info(s"Spark session summarize: \n${config.toDebugString}")
    this.spark = SparkSession
      .builder()
      .config(this.config)
      .getOrCreate()

    this
  }

  private def configureGeneralProperties(): this.type = {
    log.debug("Set general properties")

    if (appName != null) {
      this.config.setAppName(appName)
    } else {
      throw new NoSuchElementException("No AppName was found.")
    }

    this
  }

  private def configureEnvironmentProperties(): this.type = {
    log.debug("Set environment related properties")
    log.debug(s"Detect $appEnv environment")
    appEnv match {
      case AppEnv.DEV =>
        this.config.setMaster("local[*]")
      case _ =>
    }

    this
  }

  /**
    * Set the name of spark application
    *
    * @param name name of app
    * @return
    */
  def setAppName(name: String): this.type = {
    log.debug(s"Set application name to $name")
    appName = name
    this
  }

  /**
    * Set the application envir
    *
    * @param env environment of app
    * @return
    */
  def setEnv(env: String): this.type = {
    log.debug(s"Set application environment to $env")
    appEnv = AppEnv.valueOf(env.toUpperCase())
    this
  }

  def setEnv(env: AppEnv): this.type = {
    appEnv = env
    this
  }

  /**
    * Set the application envir
    *
    * @param host cassandra host
    * @return
    */
  def setCassandraHost(host: String): this.type = {
    log.debug(s"Set cassandra host to $host")
    cassandraHost = host
    this
  }

  /**
    * Override the existing configuration with an user defined configuration
    *
    * @param conf spark configuration
    * @return
    */
  def configure(conf: SparkConf): this.type = {
    log.info("Set customized spark configuration")
    this.config = conf
    this.initialization = false
    this
  }

  def reInitializeSparkConf(): this.type = {
    this.initialization = true
    this
  }

  /**
    * Build a spark session with the current configuration
    *
    * @return spark session
    */
  def get(): SparkSession = this.spark
}
