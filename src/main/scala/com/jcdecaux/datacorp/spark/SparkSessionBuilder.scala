package com.jcdecaux.datacorp.spark

import com.jcdecaux.datacorp.spark.factory.Builder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Configure and build new sparkSession according to given usages
  *
  * <br>Usage:
  *
  * {{{
  *   # Auto-configure
  *   val spark: SparkSession = new SparkSessionBuilder("cassandra", "postgres").process().get()
  *
  *   # Build with your own SparkConf
  *   val spark: SparkSession = new SparkSessionBuilder().configure(yourSparkConf).process().get()
  * }}}
  *
  * @param usages usages of the sparkSession, could be a list of the following elements:
  *               <ul>
  *               <li>cassandra</li>
  *               <li>TODO</li>
  *               </ul>
  */
class SparkSessionBuilder(usages: String*) extends Builder[SparkSession] {

  final private val logger: Logger = Logger.getLogger(this.getClass)

  var appName: String = _
  var appEnv: String = _
  var cassandraHost: String = _
  var config: SparkConf = new SparkConf()
  var initialization: Boolean = true
  var spark: SparkSession = _


  /**
    * Automatic configuration according to the settings
    *
    * @return
    */
  def build(): this.type = {
    if (initialization) {
      logger.debug("Initialize spark config")
      this.config = new SparkConf()
    }

    this.configureGeneralProperties()

    this.configureEnvironmentProperties()

    usages.toSet.foreach((usage: String) => {
      usage match {
        case "cassandra" => this.config.set("spark.cassandra.connection.host", cassandraHost)
        case "test" => logger.warn("Testing usage")
        case _ => logger.error(s"Skip unknown usage: $usage")
      }
    })

    logger.info(s"Spark session summarize: \n${config.toDebugString}")
    this.spark = SparkSession
      .builder()
      .config(this.config)
      .getOrCreate()

    this
  }

  /**
    * Set the name of spark application
    *
    * @param name name of app
    * @return
    */
  def setAppName(name: String): this.type = {
    logger.debug(s"Set application name to $name")
    appName = name
    this
  }

  /**
    * Set the application envir
    *
    * @param name name of app
    * @return
    */
  def setEnv(env: String): this.type = {
    logger.debug(s"Set application environment to $env")
    appEnv = env
    this
  }

  /**
    * Set the application envir
    *
    * @param name name of app
    * @return
    */
  def setCassandraHost(host: String): this.type = {
    logger.debug(s"Set cassandra host to $host")
    cassandraHost = host
    this
  }

  private def configureGeneralProperties(): this.type = {
    logger.debug("Set general properties")
    this.config.setAppName(appName)
    this
  }

  private def configureEnvironmentProperties(): this.type = {
    logger.debug("Set environment related properties")
    logger.debug(s"Detect $appEnv environment")
    appEnv match {
      case "dev" =>
        this.config.setMaster("local[*]")
    }

    this
  }

  /**
    * Override the existing configuration with an user defined configuration
    *
    * @param conf spark configuration
    * @return
    */
  def configure(conf: SparkConf): this.type = {
    this.logger.info("Set customized spark configuration")
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
