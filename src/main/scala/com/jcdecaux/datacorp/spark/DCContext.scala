package com.jcdecaux.datacorp.spark

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.ConfigLoader
import com.jcdecaux.datacorp.spark.storage.connector.Connector
import com.jcdecaux.datacorp.spark.storage.repository.SparkRepository
import com.jcdecaux.datacorp.spark.storage.{ConnectorBuilder, SparkRepositoryBuilder}
import com.jcdecaux.datacorp.spark.transformation.Deliverable
import com.jcdecaux.datacorp.spark.workflow.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.{universe => ru}

@InterfaceStability.Unstable
abstract class DCContext(val configLoader: ConfigLoader) {

  val spark: SparkSession

  private[this] val sparkRepositoryRegister: ConcurrentHashMap[String, Deliverable[_]] = new ConcurrentHashMap()
  private[this] val pipelineRegister: ConcurrentHashMap[UUID, Pipeline] = new ConcurrentHashMap()

  /**
    * Get a SparkRepository[DT]
    *
    * @param config path to spark repository configuration
    * @tparam DT type of spark repository
    * @return
    */
  def getSparkRepository[DT: ru.TypeTag](config: String): SparkRepository[DT] = {
    setSparkRepository[DT](config)
    sparkRepositoryRegister.get(config).payload.asInstanceOf[SparkRepository[DT]]
  }

  def resetSparkRepository[DT: ru.TypeTag](config: String): this.type = {
    val repo = new SparkRepositoryBuilder[DT](configLoader.getConfig(config)).setSpark(spark).getOrCreate()
    val deliverable = new Deliverable(repo)
    sparkRepositoryRegister.put(config, deliverable)
    this
  }

  def setSparkRepository[DT: ru.TypeTag](config: String): this.type = {
    if (!sparkRepositoryRegister.contains(config)) resetSparkRepository[DT](config)
    this
  }

  def getConnector(config: String): Connector = {
    new ConnectorBuilder(spark, configLoader.getConfig(config)).getOrCreate()
  }

  def getSparkSession: SparkSession = this.spark

  def newPipeline(): Pipeline = {
    val _pipe = new Pipeline
    pipelineRegister.put(_pipe.getUUID, _pipe)
    import scala.collection.JavaConverters._
    sparkRepositoryRegister.asScala.foreach { case (_, del) => _pipe.setInput(del) }
    _pipe
  }

  def getPipeline(uuid: UUID): Pipeline = this.pipelineRegister.get(uuid)

}

object DCContext {

  class Builder extends com.jcdecaux.datacorp.spark.Builder[DCContext] {

    private[this] var dcContext: DCContext = _
    private[this] var dcContextConfiguration: String = _
    private[this] var configLoader: ConfigLoader = _
    private[this] var sparkConf: Option[SparkConf] = None

    def setDCContextConfigPath(config: String): this.type = {
      dcContextConfiguration = config
      this
    }

    def setSparkConf(sparkConf: SparkConf): this.type = {
      this.sparkConf = Option(sparkConf)
      this
    }

    def setConfigLoader(configLoader: ConfigLoader): this.type = {
      this.configLoader = configLoader
      this
    }

    private[this] def buildSparkSession(): SparkSession = {
      val pathOf: String => String = (s: String) => s"$dcContextConfiguration.$s"

      val usages: Array[String] = if (configLoader.has(pathOf("usages"))) {
        configLoader.getArray(pathOf("usages"))
      } else {
        Array()
      }

      val appEnv = configLoader.getOption(pathOf("appEnv"))

      val cassandraHost = configLoader.getOption(pathOf("cassandraHost"))

      val sparkSessionBuilder = new SparkSessionBuilder(usages: _*).setAppName(configLoader.appName)

      appEnv match {
        case Some(env) => sparkSessionBuilder.setEnv(env)
        case _ =>
      }

      cassandraHost match {
        case Some(cqlHost) => sparkSessionBuilder.setCassandraHost(cqlHost)
        case _ =>
      }

      sparkConf match {
        case Some(conf) => sparkSessionBuilder.configure(conf)
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

  def builder(): DCContext.Builder = new DCContext.Builder

}
