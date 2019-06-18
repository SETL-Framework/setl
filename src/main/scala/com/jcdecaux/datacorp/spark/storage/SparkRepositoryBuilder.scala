package com.jcdecaux.datacorp.spark.storage

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

// TODO : Qin should check the implementation of suffix
/**
  * The SparkRepositoryBuilder will build a [[SparkRepository]] according to the given [[DataType]] and [[Storage]]
  *
  * @param storage type of storage
  * @tparam DataType type of data
  */
class SparkRepositoryBuilder[DataType <: Product : ClassTag : TypeTag](storage: Option[Storage], var config: Option[Config])
  extends Builder[v2.repository.SparkRepository[DataType]] with Logging {

  private[spark] var keyspace: String = _
  private[spark] var table: String = _
  private[spark] var spark: Option[SparkSession] = None
  private[spark] var partitionKeyColumns: Option[Seq[String]] = None
  private[spark] var clusteringKeyColumns: Option[Seq[String]] = None

  private[spark] var path: String = _
  private[spark] var suffix: Option[String] = None
  private[spark] var inferSchema: String = "true"
  private[spark] var schema: Option[StructType] = None
  private[spark] var delimiter: String = ";"
  private[spark] var header: String = "true"
  private[spark] var saveMode: SaveMode = SaveMode.Overwrite

  private[spark] var dataAddress: String = "A1"
  private[spark] var treatEmptyValuesAsNulls: String = "true"
  private[spark] var addColorColumns: String = "false"
  private[spark] var timestampFormat: String = "yyyy-mm-dd hh:mm:ss.000"
  private[spark] var dateFormat: String = "yyyy-mm-dd"
  private[spark] var maxRowsInMemory: Option[Long] = None
  private[spark] var excerptSize: Long = 10
  private[spark] var workbookPassword: Option[String] = None

  private[spark] var dynamoRegion: String = _
  private[spark] var dynamoTable: String = _

  private[this] var connector: Connector = _
  private[this] var sparkRepository: v2.repository.SparkRepository[DataType] = _

  def this(storage: Storage) = this(Some(storage), None)

  def this(config: Config) = this(None, Some(config))

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
          log.debug("Build connector with configuration")
          config = Some(configuration.withValue("path", ConfigValueFactory.fromAnyRef(configuration.getString("path") + "/" + pathSuffix)))
        } catch {
          case missing: ConfigException.Missing => log.error("To use suffix please make sure you have a path in your configuration")
          case e: Throwable => throw e
        }
      case _ =>
        log.debug("No connector configuration was found. Setting suffix variable")
        suffix = Some(pathSuffix)
    }
    this
  }

  def setKeyspace(keyspace: String): this.type = {
    this.keyspace = keyspace
    this
  }

  def setTable(table: String): this.type = {
    this.table = table
    this
  }

  def setSpark(spark: SparkSession): this.type = {
    this.spark = Option(spark)
    this
  }

  def setPartitionKeys(cols: Option[Seq[String]]): this.type = {
    this.partitionKeyColumns = cols
    this
  }

  def setClusteringKeys(cols: Option[Seq[String]]): this.type = {
    this.clusteringKeyColumns = cols
    this
  }

  def setPath(path: String): this.type = {
    this.path = path
    this
  }

  def inferSchema(boo: Boolean): this.type = {
    if (boo) {
      this.inferSchema = "true"
    } else {
      this.inferSchema = "false"
    }

    this
  }

  def setSchema(schema: StructType): this.type = {
    this.schema = Some(schema)
    this
  }

  def setDelimiter(delimiter: String): this.type = {
    this.delimiter = delimiter
    this
  }

  def header(boo: Boolean): this.type = {
    this.header = if (boo) "true" else "false"
    this
  }

  def setSaveMode(saveMode: SaveMode): this.type = {
    this.saveMode = saveMode
    this
  }

  def setDataAddress(address: String): this.type = {
    this.dataAddress = address
    this
  }

  def treatEmptyValuesAsNulls(boo: Boolean): this.type = {
    if (boo) {
      this.treatEmptyValuesAsNulls = "true"
    } else {
      this.treatEmptyValuesAsNulls = "false"
    }
    this
  }

  def addColorColumns(boo: Boolean): this.type = {
    if (boo) {
      this.addColorColumns = "true"
    } else {
      this.addColorColumns = "false"
    }
    this
  }

  def setTimestampFormat(fmt: String): this.type = {
    this.timestampFormat = fmt
    this
  }

  def setDateFormat(fmt: String): this.type = {
    this.dateFormat = fmt
    this
  }

  def setMaxRowsInMemory(maxRowsInMemory: Option[Long]): this.type = {
    this.maxRowsInMemory = maxRowsInMemory
    this
  }

  def setExcerptSize(size: Long): this.type = {
    this.excerptSize = size
    this
  }

  def setWorkbookPassword(pwd: Option[String]): this.type = {
    this.workbookPassword = pwd
    this
  }

  def getSuffix: String = {
    if(suffix.isDefined) "/" + suffix.get else ""
  }

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
        new CassandraConnector(
          keyspace = keyspace,
          table = table,
          spark = spark.get,
          partitionKeyColumns = partitionKeyColumns,
          clusteringKeyColumns = clusteringKeyColumns
        )

      case Some(Storage.CSV) =>
        log.info("Detect CSV storage")
        new CSVConnector(
          spark = spark.get,
          path = path + getSuffix,
          inferSchema = inferSchema,
          delimiter = delimiter,
          header = header,
          saveMode = saveMode
        )

      case Some(Storage.PARQUET) =>
        log.info("Detect Parquet storage")
        new ParquetConnector(
          spark = spark.get,
          path = path + getSuffix,
          table = table,
          saveMode = saveMode
        )

      case Some(Storage.EXCEL) =>
        log.info("Detect excel storage")

        if (inferSchema.toBoolean & schema.isEmpty) {
          log.warn("Excel connect may not behave as expected when parsing/saving Integers. " +
            "It's recommended to define a schema instead of infer one")
        }

        new ExcelConnector(
          spark = spark.get,
          path = path + getSuffix,
          useHeader = header,
          dataAddress = dataAddress,
          treatEmptyValuesAsNulls = treatEmptyValuesAsNulls,
          inferSchema = inferSchema,
          addColorColumns = addColorColumns,
          timestampFormat = timestampFormat,
          dateFormat = dateFormat,
          maxRowsInMemory = maxRowsInMemory,
          excerptSize = excerptSize,
          workbookPassword = workbookPassword,
          schema = schema,
          saveMode = saveMode
        )

      case Some(Storage.DYNAMODB) =>
        log.info("Detect dynamodb storage")
        new DynamoDBConnector(
          spark = spark.get,
          region = dynamoRegion,
          table = dynamoTable,
          saveMode = saveMode
        )

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
