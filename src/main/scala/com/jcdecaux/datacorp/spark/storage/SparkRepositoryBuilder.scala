package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.config.Conf.Serializer
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.factory.Builder
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.storage.v2.connector._
import com.typesafe.config.{Config, ConfigException, ConfigValueFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * The SparkRepositoryBuilder will build a [[SparkRepository]] according to the given [[DataType]] and [[Storage]]
  *
  * @param spark   spark session
  * @param storage type of storage
  * @param config  a [[com.typesafe.config.Config]] object
  * @tparam DataType type of data
  */
class SparkRepositoryBuilder[DataType <: Product : ClassTag : TypeTag](var spark: Option[SparkSession],
                                                                       var storage: Option[Storage],
                                                                       var config: Option[Config])
  extends Builder[v2.repository.SparkRepository[DataType]] with Logging {

  import Conf.Serializer._

  def this() = this(None, None, None)

  def this(storage: Storage) = this(None, Some(storage), None)

  def this(config: Config) = this(None, None, Some(config))

  private[this] val conf: Conf = new Conf()

  if (config.isEmpty) {
    set("inferSchema", true)
    set("delimiter", ";")
    set("useHeader", true)
    set("header", true)
    set("saveMode", "Overwrite")
    set("dataAddress", "A1")
    set("treatEmptyValuesAsNulls", true)
    set("addColorColumns", false)
    set("timestampFormat", "yyyy-mm-dd hh:mm:ss.000")
    set("dateFormat", "yyyy-mm-dd")
    set("excerptSize", 10L)
  }

  private[this] var connector: Connector = _
  private[this] var sparkRepository: v2.repository.SparkRepository[DataType] = _

  def set[T](key: String, value: T)(implicit converter: Serializer[T]): this.type = {
    conf.set(key, value)
    this
  }

  def getAs[T](key: String)(implicit converter: Serializer[T]): Option[T] = {
    conf.getAs[T](key)
  }

  // TODO : @Mounir: here we only handle parquet/csv/excel storage.
  //                 No changes will be made for cassandra and dynamodb connector if we set a suffix
  /**
    * Only affect file storage system to get a specific path (exp : Reach -> suffix [Rome])
    *
    * @param pathSuffix
    * @return
    */
  def setSuffix(pathSuffix: String): this.type = {
    config match {
      case Some(configuration) =>
        try {
          config = Some(configuration.withValue("path", ConfigValueFactory.fromAnyRef(configuration.getString("path") + "/" + pathSuffix)))
        } catch {
          case missing: ConfigException.Missing => log.error("To use suffix please make sure you have a path in your configuration")
          case e: Throwable => throw e
        }
      case _ =>
        log.debug("No connector configuration was found. Setting suffix variable")
        set("path", s"${getAs[String]("path").get}/$pathSuffix")
    }
    this
  }

  def setSpark(spark: SparkSession): this.type = {
    this.spark = Option(spark)
    this
  }

  def setKeyspace(keyspace: String): this.type = set("keyspace", keyspace)

  def setTable(table: String): this.type = set("table", table)

  def setPartitionKeys(cols: Option[Seq[String]]): this.type = set("partitionKeyColumns", cols.get.toArray)

  def setClusteringKeys(cols: Option[Seq[String]]): this.type = set("clusteringKeyColumns", cols.get.toArray)

  def setPath(path: String): this.type = set("path", path)

  def setInferSchema(boo: Boolean): this.type = set("inferSchema", boo)

  def setSchema(schema: StructType): this.type = set("schema", schema.toDDL)

  def setDelimiter(delimiter: String): this.type = set("delimiter", delimiter)

  def setUseHeader(boo: Boolean): this.type = set("useHeader", boo)

  def setHeader(boo: Boolean): this.type = set("header", boo)

  def setSaveMode(saveMode: SaveMode): this.type = set("saveMode", saveMode.toString)

  def setDataAddress(address: String): this.type = set("dataAddress", address)

  def setTreatEmptyValuesAsNulls(boo: Boolean): this.type = set("treatEmptyValuesAsNulls", boo)

  def setAddColorColumns(boo: Boolean): this.type = set("addColorColumns", boo)

  def setTimestampFormat(fmt: String): this.type = set("timestampFormat", fmt)

  def setDateFormat(fmt: String): this.type = set("dateFormat", fmt)

  def setMaxRowsInMemory(maxRowsInMemory: Long): this.type = set("maxRowsInMemory", maxRowsInMemory)

  def setExcerptSize(size: Long): this.type = set("excerptSize", size)

  def setWorkbookPassword(pwd: String): this.type = set("workbookPassword", pwd)

  /**
    * Build an object
    *
    * @return
    */
  override def build(): SparkRepositoryBuilder.this.type = {
    if (connector == null) {
      log.info("No user-defined connector, create one according to the storage type")

      connector = createConnector()
    }
    sparkRepository = new v2.repository.SparkRepository[DataType].setConnector(connector)
    this
  }

  /**
    * Create the connector according to the storage type
    *
    * @return [[Connector]]
    */
  protected[this] def createConnector(): Connector = {

    spark match {
      case None => throw new NullPointerException("SparkSession is not defined")
      case _ =>
    }

    config match {
      case Some(configuration) =>
        try {
          log.debug("Build connector with configuration")
          return new ConnectorBuilder(spark.get, configuration).build().get()
        } catch {
          case unknown: UnknownException.Storage => log.error("Unknown storage type in connector configuration")
          case e: Throwable => throw e
        }

      case _ => log.debug("No connector configuration was found, build with parameters")
    }

    this.storage match {
      case Some(Storage.CASSANDRA) =>
        log.info("Detect Cassandra storage")
        new CassandraConnector(spark.get, conf)

      case Some(Storage.CSV) =>
        log.info("Detect CSV storage")
        new CSVConnector(spark.get, conf)

      case Some(Storage.PARQUET) =>
        log.info("Detect Parquet storage")
        new ParquetConnector(spark.get, conf)

      case Some(Storage.EXCEL) =>
        log.info("Detect excel storage")

        if (getAs[Boolean]("inferSchema").get & getAs[String]("schema").isEmpty) {
          log.warn("Excel connect may not behave as expected when parsing/saving Integers. " +
            "It's recommended to define a schema instead of infer one")
        }

        new ExcelConnector(spark.get, conf)

      case Some(Storage.DYNAMODB) =>
        log.info("Detect dynamodb storage")
        new DynamoDBConnector(spark.get, conf)

      case _ => throw new UnknownException.Storage("The current storage is not supported")
    }
  }

  def setConnector(connector: Connector): this.type = {
    log.info(s"Set user-defined ${connector.getClass} connector")
    this.connector = connector
    this
  }

  /**
    * Get the built spark repository
    *
    * @return [[SparkRepository]]
    */
  override def get(): v2.repository.SparkRepository[DataType] = this.sparkRepository
}
