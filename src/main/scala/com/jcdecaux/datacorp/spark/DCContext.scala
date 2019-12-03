package com.jcdecaux.datacorp.spark

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.ConfigLoader
import com.jcdecaux.datacorp.spark.storage.connector.Connector
import com.jcdecaux.datacorp.spark.storage.repository.SparkRepository
import com.jcdecaux.datacorp.spark.storage.{ConnectorBuilder, SparkRepositoryBuilder}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}
import com.jcdecaux.datacorp.spark.workflow.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.{universe => ru}
import scala.util.Random

@InterfaceStability.Evolving
abstract class DCContext(val configLoader: ConfigLoader) {

  val spark: SparkSession

  private[this] val inputRegister: ConcurrentHashMap[String, Deliverable[_]] = new ConcurrentHashMap()
  private[this] val pipelineRegister: ConcurrentHashMap[UUID, Pipeline] = new ConcurrentHashMap()

  private[this] val repositoryId: String => String = config => s"@rpstry.$config"
  private[this] val connectorId: String => String = config => s"@cnnctr.$config"

  /**
    * Get a SparkRepository[DT]. If the given config path hasn't been registered, then the repository will
    * firstly be registered and then be returned.
    *
    * @param config path to spark repository configuration
    * @tparam DT type of spark repository
    * @return
    */
  def getSparkRepository[DT: ru.TypeTag](config: String): SparkRepository[DT] = {
    setSparkRepository[DT](config)
    inputRegister.get(repositoryId(config)).payload.asInstanceOf[SparkRepository[DT]]
  }

  /**
    * Force register a spark repository for the given config path. If there this config path has been registered,
    * it will be updated
    *
    * @param config     path to spark repository configuration
    * @param consumer   Seq of consumer
    * @param deliveryId id of this delivery that will be used during the delivery matching
    * @tparam DT type of spark repository
    * @return
    */
  def resetSparkRepository[DT: ru.TypeTag](config: String,
                                           consumer: Seq[Class[_ <: Factory[_]]] = Seq.empty,
                                           deliveryId: String = Deliverable.DEFAULT_ID,
                                           readCache: Boolean = false): this.type = {
    val repo = new SparkRepositoryBuilder[DT](configLoader.getConfig(config)).setSpark(spark).getOrCreate().persistReadData(readCache)
    val deliverable = new Deliverable(repo).setConsumers(consumer).setDeliveryId(deliveryId)
    inputRegister.put(repositoryId(config), deliverable)
    this
  }

  /**
    * Register a spark repository for the given config path. If there this config path has been registered,
    * it will NOT be updated
    *
    * @param config     path to spark repository configuration
    * @param consumer   Seq of consumer
    * @param deliveryId id of this delivery that will be used during the delivery matching
    * @tparam DT type of spark repository
    * @return
    */
  def setSparkRepository[DT: ru.TypeTag](config: String,
                                         consumer: Seq[Class[_ <: Factory[_]]] = Seq.empty,
                                         deliveryId: String = Deliverable.DEFAULT_ID,
                                         readCache: Boolean = false): this.type = {
    if (!inputRegister.contains(repositoryId(config))) resetSparkRepository[DT](config, consumer, deliveryId, readCache)
    this
  }

  /**
    * Get a Connector. If the given config path hasn't been registered, then the connector will
    * firstly be registered and then be returned.
    *
    * @param config path to connector configuration
    * @tparam CN type of the connector
    * @return
    */
  def getConnector[CN <: Connector](config: String): CN = {
    new ConnectorBuilder(spark, configLoader.getConfig(config)).getOrCreate().asInstanceOf[CN]
  }

  /**
    * Register a connector. As each connector must have an delivery ID, by default the config path will be used.
    *
    * <p>If there this config path has been registered, it will NOT be updated.</p>
    *
    * @param config path to connector configuration
    * @return
    */
  def setConnector(config: String): this.type = this.setConnector(config, config)

  def setConnector[CN <: Connector : ru.TypeTag](config: String, cls: Class[CN]): this.type =
    this.setConnector(config, config, classOf[Connector])

  /**
    * Register a connector.
    *
    * <p>If there this config path has been registered, it will NOT be updated.</p>
    *
    * @param config     path to connector configuration
    * @param deliveryId delivery ID
    * @return
    */
  def setConnector(config: String, deliveryId: String): this.type = {
    if (!inputRegister.contains(connectorId(config))) resetConnector(config, deliveryId, classOf[Connector])
    this
  }

  /**
    * Register a connector.
    *
    * <p>If there this config path has been registered, it will NOT be updated.</p>
    *
    * @param config     path to connector configuration
    * @param deliveryId delivery ID
    * @param cls        class of the Connector
    * @tparam CN type of spark connector
    * @return
    */
  def setConnector[CN <: Connector : ru.TypeTag](config: String, deliveryId: String, cls: Class[CN]): this.type = {
    if (!inputRegister.contains(connectorId(config))) resetConnector(config, deliveryId, cls)
    this
  }

  /**
    * Force register a connector. As each connector must have an delivery ID, by default the config path will be used.
    *
    * <p>If there this config path has been registered, it will be updated.</p>
    *
    * @param config path to connector configuration
    * @return
    */
  def resetConnector(config: String): this.type = this.resetConnector(config, config, classOf[Connector])

  /**
    * Register a connector.
    *
    * <p>If there this config path has been registered, it will be updated.</p>
    *
    * @param config     path to connector configuration
    * @param deliveryId delivery ID
    * @param cls        class of the Connector
    * @tparam CN type of spark connector
    * @return
    */
  def resetConnector[CN <: Connector : ru.TypeTag](config: String, deliveryId: String, cls: Class[CN]): this.type = {
    val payload = new ConnectorBuilder(spark, configLoader.getConfig(config)).getOrCreate().asInstanceOf[CN]
    val deliverable = new Deliverable(payload).setDeliveryId(deliveryId)
    inputRegister.put(connectorId(config), deliverable)
    this
  }


  def sparkSession: SparkSession = this.spark

  /**
    * Create a new pipeline. All the registered repositories and connectors will be passed into the delivery pool
    * of the pipeline.
    *
    * @return
    */
  def newPipeline(): Pipeline = {
    val _pipe = new Pipeline
    pipelineRegister.put(_pipe.getUUID, _pipe)
    import scala.collection.JavaConverters._
    inputRegister.asScala.foreach { case (_, del) => _pipe.setInput(del) }
    _pipe
  }

  /**
    * Find a pipeline by its UUID
    *
    * @param uuid UUID of the target pipeline
    * @return
    */
  def getPipeline(uuid: UUID): Pipeline = this.pipelineRegister.get(uuid)

  /**
    * Stop the spark session
    */
  def stop(): Unit = {
    this.spark.stop()
  }
}

object DCContext {

  class Builder extends com.jcdecaux.datacorp.spark.Builder[DCContext] {

    private[this] var dcContext: DCContext = _
    private[this] var dcContextConfiguration: String = _
    private[this] var configLoader: ConfigLoader = _
    private[this] var sparkConf: Option[SparkConf] = None
    private[this] var parallelism: Int = 200
    private[this] var sparkMasterUrl: Option[String] = None

    def setDCContextConfigPath(config: String): this.type = {
      dcContextConfiguration = config
      this
    }

    def setSparkConf(sparkConf: SparkConf): this.type = {
      this.sparkConf = Option(sparkConf)
      this
    }

    def setParallelism(par: Int): this.type = {
      this.parallelism = par
      this
    }

    def setConfigLoader(configLoader: ConfigLoader): this.type = {
      this.configLoader = configLoader
      this
    }

    def setSparkMaster(url: String): this.type = {
      this.sparkMasterUrl = Option(url)
      this
    }

    def withDefaultConfigLoader(configPath: String): this.type = {
      this.configLoader = ConfigLoader.builder()
        .setAppName(sparkAppName)
        .setConfigPath(configPath)
        .getOrCreate()
      this
    }

    def withDefaultConfigLoader(): this.type = {
      this.configLoader = ConfigLoader.builder()
        .setAppName(sparkAppName)
        .getOrCreate()
      this
    }

    private[this] val sparkAppName: String = s"dc_spark_app_${Random.alphanumeric.take(10).mkString("")}"

    private[this] def buildSparkSession(): SparkSession = {
      val pathOf: String => String = (s: String) => s"$dcContextConfiguration.$s"

      val usages: Array[String] = if (configLoader.has(pathOf("usages"))) {
        configLoader.getArray(pathOf("usages"))
      } else {
        Array()
      }

      val cassandraHost = configLoader.getOption(pathOf("cassandraHost"))

      val sparkSessionBuilder = new SparkSessionBuilder(usages: _*)
        .setAppName(configLoader.appName)
        .setEnv(configLoader.appEnv)
        .setParallelism(parallelism)

      cassandraHost match {
        case Some(cqlHost) => sparkSessionBuilder.setCassandraHost(cqlHost)
        case _ =>
      }

      sparkConf match {
        case Some(conf) => sparkSessionBuilder.withSparkConf(conf)
        case _ =>
      }

      sparkMasterUrl match {
        case Some(url) => sparkSessionBuilder.setSparkMaster(url)
        case _ =>
      }

      sparkSessionBuilder.getOrCreate()
    }


    /**
      * Build an object
      *
      * @return
      */
    override def build(): Builder.this.type = {
      dcContext = new DCContext(configLoader) {
        override val spark: SparkSession = buildSparkSession()
      }
      this
    }

    override def get(): DCContext = dcContext
  }

  def builder(): DCContext.Builder = new DCContext.Builder()

}
