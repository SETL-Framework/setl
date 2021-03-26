package io.github.setl

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.github.setl.annotation.InterfaceStability
import io.github.setl.config.ConfigLoader
import io.github.setl.internal.HasRegistry
import io.github.setl.storage.connector.Connector
import io.github.setl.storage.repository.SparkRepository
import io.github.setl.storage.{ConnectorBuilder, SparkRepositoryBuilder}
import io.github.setl.transformation.{Deliverable, Factory}
import io.github.setl.util.TypesafeConfigUtils
import io.github.setl.workflow.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.{universe => ru}
import scala.util.Random

@InterfaceStability.Evolving
abstract class Setl(val configLoader: ConfigLoader) extends HasRegistry[Pipeline] {

  val spark: SparkSession

  private[this] val externalInputRegistry: ConcurrentHashMap[String, Deliverable[_]] = new ConcurrentHashMap()
  private[setl] val repositoryIdOf: String => String = config => s"@rpstry.$config"
  private[setl] val connectorIdOf: String => String = config => s"@cnnctr.$config"

  private[setl] def hasExternalInput(id: String): Boolean = externalInputRegistry.containsKey(id)

  private[setl] def listInputs(): Map[String, Deliverable[_]] = {
    import scala.collection.JavaConverters._
    externalInputRegistry.asScala.toMap
  }

  /**
   * Get a SparkRepository[DT]. If the given config path hasn't been registered, then the repository will
   * firstly be registered and then be returned.
   *
   * @param repositoryId path to spark repository configuration
   * @tparam DT type of spark repository
   * @return the added repository
   */
  def getSparkRepository[DT: ru.TypeTag](repositoryId: String): SparkRepository[DT] = {
    setSparkRepository[DT](repositoryId)
    externalInputRegistry.get(repositoryIdOf(repositoryId)).getPayload.asInstanceOf[SparkRepository[DT]]
  }

  /**
   * Force register a spark repository with an object of SparkRepository and its id. If a repository having
   * the same ID was already registered, it will be overwritten by this one.
   *
   * @param repository   an object of SparkRepository[T]
   * @param consumer     consumer of this spark repository
   * @param deliveryId   id of this delivery
   * @param repositoryId id to be used for the repository registration
   * @tparam DT data type of the repository
   * @return the current SETL context with the added repository
   */
  def resetSparkRepository[DT: ru.TypeTag](repository: SparkRepository[DT],
                                           consumer: Seq[Class[_ <: Factory[_]]],
                                           deliveryId: String,
                                           repositoryId: String): this.type = {
    val deliverable = new Deliverable(repository).setConsumers(consumer).setDeliveryId(deliveryId)
    externalInputRegistry.put(repositoryIdOf(repositoryId), deliverable)
    this
  }

  /**
   * Force register a spark repository for the given config path. If there this config path has been registered,
   * it will be updated
   *
   * @param config     path to spark repository configuration
   * @param consumer   Seq of consumer
   * @param deliveryId id of this delivery that will be used during the delivery matching
   * @param cacheData  default false, if set to true, then the SparkRepository will cache the data after reading it
   * @tparam DT type of spark repository
   * @return the current SETL context with the added repository
   */
  def resetSparkRepository[DT: ru.TypeTag](config: String,
                                           consumer: Seq[Class[_ <: Factory[_]]] = Seq.empty,
                                           deliveryId: String = Deliverable.DEFAULT_ID,
                                           cacheData: Boolean = false): this.type = {
    val repo = new SparkRepositoryBuilder[DT](configLoader.getConfig(config)).getOrCreate().persistReadData(cacheData)
    resetSparkRepository(repo, consumer, deliveryId, config)
    this
  }

  /**
   * Register a spark repository for the given config path. If there this config path has been registered,
   * it will NOT be updated
   *
   * @param config     path to spark repository configuration
   * @param consumer   Seq of consumer
   * @param deliveryId id of this delivery that will be used during the delivery matching
   * @param cacheData  default false, if set to true, then the SparkRepository will cache the data after reading it
   * @tparam DT type of spark repository
   * @return the current SETL context with the added repository
   */
  def setSparkRepository[DT: ru.TypeTag](config: String,
                                         consumer: Seq[Class[_ <: Factory[_]]] = Seq.empty,
                                         deliveryId: String = Deliverable.DEFAULT_ID,
                                         cacheData: Boolean = false): this.type = {
    if (!hasExternalInput(repositoryIdOf(config))) {
      resetSparkRepository[DT](config, consumer, deliveryId, cacheData)
    }
    this
  }

  /**
   * Register a spark repository with an object of SparkRepository and its id. If a repository having
   * the same ID was already registered, it will NOT be overwritten by this one.
   *
   * @param repository   an object of SparkRepository[T]
   * @param consumer     consumer of this spark repository
   * @param deliveryId   id of this delivery
   * @param repositoryId id to be used for the repository registration
   * @tparam DT data type of the repository
   * @return the current SETL context with the added repository
   */
  def setSparkRepository[DT: ru.TypeTag](repository: SparkRepository[DT],
                                         consumer: Seq[Class[_ <: Factory[_]]],
                                         deliveryId: String,
                                         repositoryId: String): this.type = {
    if (!hasExternalInput(repositoryIdOf(repositoryId))) {
      resetSparkRepository[DT](repository, consumer, deliveryId, repositoryId)
    }
    this
  }

  /**
   * Get a Connector. If the given config path hasn't been registered, then the connector will
   * firstly be registered and then be returned.
   *
   * @param connectorId id of connector (could be the config path)
   * @tparam CN type of the connector
   * @return the registered connector
   */
  def getConnector[CN <: Connector](connectorId: String): CN = {
    setConnector(connectorId)
    externalInputRegistry.get(connectorIdOf(connectorId)).getPayload.asInstanceOf[CN]
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

  /**
   * Register a connector.
   *
   * <p>If there this config path has been registered, it will NOT be updated.</p>
   *
   * @param config     path to connector configuration
   * @param deliveryId delivery ID
   * @return the current SETL context with the added connector
   */
  def setConnector(config: String, deliveryId: String): this.type = {
    if (!hasExternalInput(connectorIdOf(config))) resetConnector[Connector](config, deliveryId, classOf[Connector])
    this
  }

  /**
   * Register a connector. As each connector must have an delivery ID, by default the config path will be used.
   *
   * <p>If there this config path has been registered, it will NOT be updated.</p>
   *
   * @param config path to connector configuration
   * @tparam CN type of connector
   * @return the current SETL context with the added repository
   */
  def setConnector[CN <: Connector : ru.TypeTag](config: String, cls: Class[CN]): this.type =
    this.setConnector[CN](config, config, cls)


  /**
   * Register a connector.
   *
   * <p>If there this config path has been registered, it will NOT be updated.</p>
   *
   * @param config     path to connector configuration
   * @param deliveryId delivery ID
   * @param cls        class of the Connector
   * @tparam CN type of spark connector
   * @return the current SETL context with the added repository
   */
  def setConnector[CN <: Connector : ru.TypeTag](config: String, deliveryId: String, cls: Class[CN]): this.type = {
    if (!hasExternalInput(connectorIdOf(config))) resetConnector[CN](config, deliveryId, cls)
    this
  }

  /**
   * Register a connector.
   *
   * <p>If there this config path has been registered, it will NOT be updated.</p>
   *
   * @param connector   a connector
   * @param deliveryId  delivery ID
   * @param connectorId id of the Connector
   * @tparam CN type of spark connector
   * @return the current SETL context with the added repository
   */
  def setConnector[CN <: Connector : ru.TypeTag](connector: CN, deliveryId: String, connectorId: String): this.type = {
    if (!hasExternalInput(connectorIdOf(connectorId))) {
      resetConnector[CN](connector, deliveryId, connectorId)
    }
    this
  }

  /**
   * Register a connector.
   *
   * <p>If there this config path has been registered, it will be updated.</p>
   *
   * @param configPath path to connector configuration
   * @param deliveryId delivery ID
   * @param cls        class of the Connector
   * @tparam CN type of spark connector
   * @return the current SETL context with the added repository
   */
  def resetConnector[CN <: Connector : ru.TypeTag](configPath: String, deliveryId: String, cls: Class[CN]): this.type = {
    val payload = new ConnectorBuilder(configLoader.getConfig(configPath)).getOrCreate().asInstanceOf[CN]
    resetConnector[CN](payload, deliveryId, configPath)
  }

  /**
   * Register a connector.
   *
   * <p>If there this config path has been registered, it will be updated.</p>
   *
   * @param connector  a connector
   * @param deliveryId delivery ID
   * @tparam CN type of spark connector
   * @return the current SETL context with the added repository
   */
  def resetConnector[CN <: Connector : ru.TypeTag](connector: CN, deliveryId: String, connectorId: String): this.type = {
    val deliverable = new Deliverable(connector).setDeliveryId(deliveryId)
    externalInputRegistry.put(connectorIdOf(connectorId), deliverable)
    this
  }

  /** Return the current spark session */
  def sparkSession: SparkSession = this.spark

  /**
   * Create a new pipeline. All the registered repositories and connectors will be passed into the delivery pool
   * of the pipeline.
   *
   * @return a newly instantiated pipeline object
   */
  def newPipeline(): Pipeline = {
    val _pipe = new Pipeline
    super.registerNewItem(_pipe)

    import scala.collection.JavaConverters._
    externalInputRegistry.asScala.foreach { case (_, del) => _pipe.setInput(del) }
    _pipe
  }

  /**
   * Find a pipeline by its UUID
   *
   * @param uuid UUID of the target pipeline
   * @return
   */
  def getPipeline(uuid: UUID): Option[Pipeline] = super.getRegisteredItem(uuid)

  /** Stop the spark session */
  def stop(): Unit = {
    this.spark.stop()
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
  }
}

object Setl {

  class Builder extends io.github.setl.Builder[Setl] {

    private[this] var setl: Option[Setl] = None
    private[this] var contextConfiguration: Option[String] = None
    private[this] var configLoader: Option[ConfigLoader] = None
    private[this] var sparkConf: Option[SparkConf] = None
    private[this] var shufflePartitions: Option[Int] = None
    private[this] var sparkMasterUrl: Option[String] = None
    private[this] var sparkSession: Option[SparkSession] = None

    private[this] val fallbackContextConfiguration: String = "setl.config"

    /**
     * Define the config path of SETL
     *
     * @param config config path
     * @return the current builder
     */
    def setSetlConfigPath(config: String): this.type = {
      contextConfiguration = Option(config)
      this
    }

    /**
     * Define a user-defined SparkConf
     *
     * @param sparkConf SparkConf object
     * @return the current builder
     */
    def setSparkConf(sparkConf: SparkConf): this.type = {
      this.sparkConf = Option(sparkConf)
      this
    }

    /**
     * Overwrite the default Spark parallelism (200)
     *
     * @param par value of parallelism
     * @return the current builder
     */
    def setShufflePartitions(par: Int): this.type = {
      this.shufflePartitions = Some(par)
      this
    }

    /**
     * Provide a user-defined config loader
     *
     * @param configLoader ConfigLoader object
     * @return the current builder
     */
    def setConfigLoader(configLoader: ConfigLoader): this.type = {
      this.configLoader = Option(configLoader)
      this
    }

    /**
     * Set the master URL of Spark
     *
     * @param url master URL of spark
     * @return the current builder
     */
    def setSparkMaster(url: String): this.type = {
      this.sparkMasterUrl = Option(url)
      this
    }

    /**
     * Set a user-provided SparkSession
     *
     * @param sparkSession use-defined SparkSession
     * @return
     */
    def setSparkSession(sparkSession: SparkSession): this.type = {
      this.sparkSession = Option(sparkSession)
      this
    }

    /**
     * Use the default config loader and load both the default application.conf and the given configuration file
     *
     * @param configFile file path string of the configuration file
     * @return the current builder
     */
    def withDefaultConfigLoader(configFile: String): this.type = {
      val cl = ConfigLoader.builder()
        .setAppName(sparkAppName)
        .setConfigPath(configFile)
        .getOrCreate()
      this.configLoader = Option(cl)
      this
    }

    /**
     * Use the default config loader and load the default configuration file (application.conf) and an additional
     * configuration file (according to the value of setl.environment in application.conf)
     *
     * @return the current builder
     */
    def withDefaultConfigLoader(): this.type = {
      val cl = ConfigLoader.builder()
        .setAppName(sparkAppName)
        .getOrCreate()
      this.configLoader = Option(cl)
      this
    }

    private[this] val sparkAppName: String = s"spark_app_${Random.alphanumeric.take(10).mkString("")}"

    /**
     * Instantiate a SparkSession object
     *
     * @return
     */
    private[this] def buildSparkSession(): SparkSession = {
      val pathOf: String => String = (s: String) => s"${contextConfiguration.getOrElse(fallbackContextConfiguration)}.$s"

      this.configLoader match {
        case Some(cl) =>
          val usages: Array[String] = if (cl.has(pathOf("usages"))) {
            cl.getArray(pathOf("usages"))
          } else {
            Array()
          }

          val sparkConfigurations: Map[String, String] = try {
            TypesafeConfigUtils.getMap(cl.getConfig(pathOf("spark")))
          } catch {
            case _: com.typesafe.config.ConfigException.Missing =>
              logWarning(s"Config path ${pathOf("spark")} doesn't exist")
              Map.empty
          }

          val cassandraHost = cl.getOption(pathOf("cassandraHost"))

          val sparkSessionBuilder = new SparkSessionBuilder(usages: _*)
            .setAppName(cl.appName) // Set the default app name
            .setEnv(cl.appEnv) // Retrieve app env

          // SparkConf has the highest priority
          configureSpark(sparkConf, sparkSessionBuilder.withSparkConf)

          // Configure Spark with properties defined in the configuration file
          sparkSessionBuilder.set(sparkConfigurations)

          // Overwrite configuration file's properties with those defined in the application
          configureSpark(cassandraHost, sparkSessionBuilder.setCassandraHost)
          configureSpark(sparkMasterUrl, sparkSessionBuilder.setSparkMaster)
          configureSpark(shufflePartitions, sparkSessionBuilder.setShufflePartitions)

          sparkSessionBuilder.getOrCreate()

        case _ => throw new NoSuchElementException("Can't find ConfigLoader")
      }
    }

    private[this] def configureSpark[T](opt: Option[T], setter: T => SparkSessionBuilder): Unit = {
      opt match {
        case Some(thing) => setter(thing)
        case _ =>
      }
    }

    /** Build SETL */
    override def build(): Builder.this.type = {
      val _spark = sparkSession match {
        case Some(ss) => ss
        case _ =>
          logDebug("Build new spark session")
          buildSparkSession()
      }

      setl = this.configLoader match {
        case Some(cl) =>
          Option(
            new Setl(cl) {
              override val spark: SparkSession = _spark
            }
          )
        case _ => throw new NoSuchElementException("Can't find ConfigLoader")
      }

      this
    }

    override def get(): Setl = setl match {
      case Some(s) => s
      case _ => throw new NoSuchElementException("Can't find Setl. You should build it first")
    }
  }

  /** Create a builder to build SETL */
  def builder(): Setl.Builder = new Setl.Builder()

}
