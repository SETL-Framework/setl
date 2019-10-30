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

@InterfaceStability.Unstable
abstract class DCContext(val configLoader: ConfigLoader) {

  val spark: SparkSession

  private[this] val inputRegister: ConcurrentHashMap[String, Deliverable[_]] = new ConcurrentHashMap()
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
    inputRegister.get(config).payload.asInstanceOf[SparkRepository[DT]]
  }

  def resetSparkRepository[DT: ru.TypeTag](config: String,
                                           consumer: Array[Class[_ <: Factory[_]]] = Array.empty): this.type = {
    val repo = new SparkRepositoryBuilder[DT](configLoader.getConfig(config)).setSpark(spark).getOrCreate()
    val deliverable = new Deliverable(repo).setConsumers(consumer: _*)
    inputRegister.put(config, deliverable)
    this
  }

  def setSparkRepository[DT: ru.TypeTag](config: String,
                                         consumer: Array[Class[_ <: Factory[_]]] = Array.empty): this.type = {
    if (!inputRegister.contains(config)) resetSparkRepository[DT](config, consumer)
    this
  }

  def getConnector[C <: Connector](config: String): C = {
    new ConnectorBuilder(spark, configLoader.getConfig(config)).getOrCreate().asInstanceOf[C]
  }

  def getSparkSession: SparkSession = this.spark

  def newPipeline(): Pipeline = {
    val _pipe = new Pipeline
    pipelineRegister.put(_pipe.getUUID, _pipe)
    import scala.collection.JavaConverters._
    inputRegister.asScala.foreach { case (_, del) => _pipe.setInput(del) }
    _pipe
  }

  def getPipeline(uuid: UUID): Pipeline = this.pipelineRegister.get(uuid)

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
