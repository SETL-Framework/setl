package com.jcdecaux.datacorp.spark

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.enums.AppEnv
import com.jcdecaux.datacorp.spark.exception.UnknownException
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

  private[this] val SPARK_MASTER: String = "spark.master"
  private[this] val CQL_HOST: String = "spark.cassandra.connection.host"
  private[this] val SPARK_APP_NAME: String = "spark.app.name"
  private[this] val properties: ConcurrentHashMap[String, String] = new ConcurrentHashMap()
  set(SPARK_APP_NAME, "SparkApplication")

  private[spark] var appEnv: AppEnv = AppEnv.LOCAL
  private[spark] var sparkConf: SparkConf = new SparkConf()
  private[spark] var initialization: Boolean = true
  private[spark] var spark: SparkSession = _

  /**
    * Automatically build a SparkSession
    *
    * @return
    */
  def build(): this.type = {

    if (initialization) {
      log.debug("Initialize spark config")
      this.sparkConf = new SparkConf()
    }

    log.debug(s"Detect $appEnv environment")
    appEnv match {
      case AppEnv.LOCAL => setSparkMaster("local")
      case _ =>
    }

    import scala.collection.JavaConverters.mapAsScalaMapConverter
    properties.asScala.foreach {
      case (k, v) => updateSparkConf(k, v)
    }

    validateSparkConf()
    this.spark = createSparkSession
    this
  }

  /**
    * Validate SparkConf according to the usage of spark session
    */
  private[this] def validateSparkConf(): Unit = {
    if (usages.contains("cassandra")) require(sparkConf.contains(CQL_HOST))
  }

  /**
    * Add a new configuration into sparkConf. If the current key already exists in sparkConf, its value
    * will NOT be updated.
    *
    * @param key   key of SparkConf
    * @param value value of SparkConf
    */
  private[this] def updateSparkConf(key: String, value: String): Unit = {
    if (!sparkConf.contains(key)) {
      sparkConf.set(key, value)
    } else {
      log.info(s"Skip spark configuration $key -> $value")
    }
  }

  /**
    * Create or get a Spark Session
    *
    * @return
    */
  private[this] def createSparkSession: SparkSession = {
    log.info(s"Spark session summarize: \n${sparkConf.toDebugString}")
    SparkSession
      .builder()
      .config(this.sparkConf)
      .getOrCreate()
  }

  /**
    * Get Spark Master URL
    *
    * @return
    */
  def sparkMasterUrl: String = get(SPARK_MASTER)

  /**
    * Set Master URL for Spark
    *
    * @param url url of master
    * @return
    */
  def setSparkMaster(url: String): this.type = set(SPARK_MASTER, url)

  /**
    * Set the name of spark application
    *
    * @param name name of app
    * @return
    */
  def setAppName(name: String): this.type = {
    log.debug(s"Set application name to $name")
    set(SPARK_APP_NAME, name)
    this
  }

  /**
    * Get Spark application name
    *
    * @return
    */
  def appName: String = get(SPARK_APP_NAME)

  /**
    * Set application environment
    *
    * @param env LOCAL, DEV, PREPROD, PROD, EMR
    * @return
    */
  def setEnv(env: String): this.type = {
    appEnv = try {
      log.debug(s"Set application environment to $env")
      AppEnv.valueOf(env.toUpperCase())
    } catch {
      case _: IllegalArgumentException =>
        throw new UnknownException.Environment(s"Unknown environment $env")
    }
    this
  }

  /**
    * Set application environment
    *
    * @param env AppEnv
    * @return
    */
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
    set(CQL_HOST, host)
    this
  }

  /**
    * Get cassandar host value
    *
    * @return
    */
  def cassandraHost: String = get(CQL_HOST)

  /**
    * Override the existing configuration with an user defined configuration
    *
    * @param conf spark configuration
    * @return
    */
  def withSparkConf(conf: SparkConf): this.type = {
    log.info("Set customized spark configuration")
    this.sparkConf = conf
    this.initialization = false
    this
  }

  /**
    * Wrapper of withSparkConf
    *
    * @param conf spark configuration
    * @return
    */
  def configure(conf: SparkConf): this.type = withSparkConf(conf)

  /**
    * Set a SparkConf property
    *
    * @param key   key of spark conf
    * @param value value of spark conf
    */
  def set(key: String, value: String): this.type = {
    properties.put(key, value)
    this
  }

  /**
    * Get a SparkConf value
    *
    * @param key key of spark conf
    * @return string if the key exists, null otherwise
    */
  def get(key: String): String = {
    properties.get(key)
  }

  /**
    * Build a spark session with the current configuration
    *
    * @return spark session
    */
  def get(): SparkSession = this.spark.newSession()
}
