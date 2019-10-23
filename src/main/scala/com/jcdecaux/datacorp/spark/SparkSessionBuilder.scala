package com.jcdecaux.datacorp.spark

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey, Compress}
import com.jcdecaux.datacorp.spark.enums.{Storage, ValueType}
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

  import SparkSessionBuilder._
  private[this] val properties: ConcurrentHashMap[String, String] = new ConcurrentHashMap()
  set(SPARK_APP_NAME, "SparkApplication")

  private[spark] var appEnv: String = "local"
  private[spark] var sparkConf: SparkConf = new SparkConf()
  private[spark] var initialization: Boolean = true
  private[spark] var spark: SparkSession = _

  private[this] var kryoRegister: Array[Class[_]] = Array(
    classOf[Storage],
    classOf[ValueType],
    classOf[Compress],
    classOf[ColumnName],
    classOf[CompoundKey]
  )

  /**
    * Automatically build a SparkSession
    *
    * @return
    */
  def build(): this.type = {

    SparkSession.getActiveSession match {
      case Some(ss) =>
        ss.stop()
        SparkSession.clearDefaultSession()
        SparkSession.clearActiveSession()

      case _ =>
    }

    if (initialization) {
      log.debug("Initialize spark config")
      this.sparkConf = new SparkConf()
    }

    log.debug(s"Detect $appEnv environment")
    appEnv.toLowerCase() match {
      case "local" => setSparkMaster("local")
      case _ =>
    }

    import scala.collection.JavaConverters.mapAsScalaMapConverter
    properties.asScala.foreach {
      case (k, v) => updateSparkConf(k, v)
    }

    if (useKryo) {
      log.debug("User Kryo serializer")
      this.sparkConf.registerKryoClasses(kryoRegister)
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
    * Set spark.sql.shuffle.partitions
    *
    * @param par default number of partition
    * @return
    */
  def setParallelism(par: Int): this.type = set(SPARK_SHUFFLE_PARTITIONS, par.toString)

  /**
    * Get spark.sql.shuffle.partitions
    *
    * @return
    */
  def getParallelism: String = get(SPARK_SHUFFLE_PARTITIONS)

  def useKryo(boo: Boolean): this.type = set(SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer")

  def useKryo: Boolean = get(SPARK_SERIALIZER) == "org.apache.spark.serializer.KryoSerializer"

  def registerClass(cls: Class[_]): this.type = {
    kryoRegister = kryoRegister :+ cls
    this
  }

  def registerClasses(cls: Array[Class[_]]): this.type = {
    kryoRegister = kryoRegister ++ cls
    this
  }

  def setKryoRegistrationRequired(boolean: Boolean): this.type = set(SPARK_KRYO_REGISTRATION_REQUIRED, boolean.toString)

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

object SparkSessionBuilder {
  val SPARK_MASTER: String = "spark.master"
  val CQL_HOST: String = "spark.cassandra.connection.host"
  val SPARK_APP_NAME: String = "spark.app.name"
  val SPARK_SHUFFLE_PARTITIONS: String = "spark.sql.shuffle.partitions"
  val SPARK_KRYO_REGISTRATION_REQUIRED: String = "spark.kryo.registrationRequired"
  val SPARK_SERIALIZER: String = "spark.serializer"
}