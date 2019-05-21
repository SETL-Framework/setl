package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.factory.Builder
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.storage.v2.connector._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * The SparkRepositoryBuilder will build a [[SparkRepository]] according to the given [[DataType]] and [[Storage]]
  *
  * @param storage type of storage
  * @tparam DataType type of data
  */
class SparkRepositoryBuilder[DataType](val storage: Storage) extends Builder[v2.repository.SparkRepository[DataType]] with Logging {

  private[spark] var keyspace: String = _
  private[spark] var table: String = _
  private[spark] var spark: SparkSession = _
  private[spark] var partitionKeyColumns: Option[Seq[String]] = None
  private[spark] var clusteringKeyColumns: Option[Seq[String]] = None

  private[spark] var path: String = _
  private[spark] var inferSchema: String = "true"
  private[spark] var schema: Option[StructType] = None
  private[spark] var delimiter: String = ";"
  private[spark] var header: String = "true"
  private[spark] var saveMode: SaveMode = SaveMode.Overwrite

  def setKeyspace(keyspace: String): this.type = {
    this.keyspace = keyspace
    this
  }

  def setTable(table: String): this.type = {
    this.table = table
    this
  }

  def setSpark(spark: SparkSession): this.type = {
    this.spark = spark
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

  private[this] var connector: Connector[DataFrame] = _
  private[this] var sparkRepository: v2.repository.SparkRepository[DataType] = _

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
  protected def createConnector(): Connector[DataFrame] = {
    this.storage match {
      case Storage.CASSANDRA =>
        log.info("Detect Cassandra storage")
        new CassandraConnector(
          keyspace = keyspace,
          table = table,
          spark = spark,
          partitionKeyColumns = partitionKeyColumns,
          clusteringKeyColumns = clusteringKeyColumns
        )

      case Storage.CSV =>
        log.info("Detect CSV storage")
        new CSVConnector(
          spark = spark,
          path = path,
          inferSchema = inferSchema,
          delimiter = delimiter,
          header = header,
          saveMode = saveMode
        )

      case Storage.PARQUET =>
        log.info("Detect Parquet storage")
        new ParquetConnector(
          spark = spark,
          path = path,
          table = table,
          saveMode = saveMode
        )

      case Storage.EXCEL =>
        log.info("Detect excel storage")

        if (inferSchema.toBoolean & schema.isEmpty) {
          log.warn("Excel connect may not behave as expected when parsing/saving Integers. " +
            "It's recommended to define a schema instead of infer one")
        }

        new ExcelConnector(
          spark = spark,
          path = path,
          useHeader = header,
          inferSchema = inferSchema,
          schema = schema,
          saveMode = saveMode
        )


      case _ => throw new NotImplementedError("The current storage is not supported")
    }
  }

  def setConnector(connector: Connector[DataFrame]): this.type = {
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
