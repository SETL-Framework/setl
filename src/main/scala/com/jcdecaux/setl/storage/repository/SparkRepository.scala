package com.jcdecaux.setl.storage.repository

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.enums.{Storage, ValueType}
import com.jcdecaux.setl.exception.{InvalidConnectorException, UnknownException}
import com.jcdecaux.setl.internal._
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.connector.{Connector, FileConnector}
import com.jcdecaux.setl.util.HasSparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.apache.spark.storage.StorageLevel

import scala.reflect.runtime.universe.TypeTag

/**
 * SparkRepository guarantee a Read-after-write consistency.
 *
 * @tparam DataType type of spark repository
 */
@InterfaceStability.Evolving
class SparkRepository[DataType: TypeTag] extends Repository[Dataset[DataType]] with Logging with HasSparkSession {

  private[this] var connector: Connector = _
  private[this] implicit val dataEncoder: Encoder[DataType] = ExpressionEncoder[DataType]
  private[this] val schema: StructType = StructAnalyser.analyseSchema[DataType]
  private[this] val lock: ReentrantLock = new ReentrantLock()

  private[this] val cacheLastReadData: AtomicBoolean = new AtomicBoolean(false)
  private[this] val flushReadCache: AtomicBoolean = new AtomicBoolean(true)
  private[this] val lastReadHashCode: AtomicInteger = new AtomicInteger(0)
  private[this] var persistenceStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  private[this] var readCache: DataFrame = spark.emptyDataFrame

  /**
   * Whether to cache or not after reading data. This only takes effect for repository that loads data
   *
   * @return if true, then this repository will cache the loaded data, otherwise, false
   */
  def persistReadData: Boolean = this.cacheLastReadData.get()

  /**
   * SparkRepository can cache (persist) the data after reading. Set to true to cache the read data.
   *
   * @param persist true to cache the read data, false otherwise
   * @return this repository instance
   */
  def persistReadData(persist: Boolean): this.type = {
    this.cacheLastReadData.set(persist)
    this
  }

  /**
   * Get the storage level of the input cache
   *
   * @return StorageLevel
   */
  def getReadCacheStorageLevel: StorageLevel = this.persistenceStorageLevel

  /**
   * Set the storage level for caching data after reading
   *
   * @param storageLevel storage level
   * @return this repository instance
   */
  def setReadCacheStorageLevel(storageLevel: StorageLevel): this.type = {
    this.persistenceStorageLevel = storageLevel
    this
  }

  /**
   * If the connector of this repository is a file connector, and user called save(df, suffix), then the repository
   * will create a sub-directory for this given suffix. The name of directory respects the naming convention
   * of HIVE partition:
   *
   * {{{
   *   ../{UserDefinedSuffixKey}={suffix}
   * }}}
   *
   * By default UserDefinedSuffixKey = "_user_defined_suffix"
   */
  def setUserDefinedSuffixKey(key: String): this.type = {
    connector match {
      case c: FileConnector => c.setUserDefinedSuffixKey(key)
      case _ => logWarning(s"Current connector doesn't support user defined suffix, skip UDS setting")
    }
    this
  }

  /**
   * If the connector of this repository is a file connector, and user called save(df, suffix), then the repository
   * will create a sub-directory for this given suffix. The name of directory respects the naming convention
   * of HIVE partition:
   *
   * {{{
   *   ../{UserDefinedSuffixKey}={suffix}
   * }}}
   *
   * By default UserDefinedSuffixKey = "_user_defined_suffix"
   */
  def setUserDefinedSuffixKey(key: Option[String]): this.type = {
    key match {
      case Some(k) => this.setUserDefinedSuffixKey(k)
      case _ => logWarning(s"Current connector doesn't support user defined suffix, skip UDS setting")
    }
    this
  }

  /**
   * Get the value of user defined suffix column name.
   *
   * @return if the connector is a FileConnector, then the user defined suffix key will be returned,
   *         by default the key is "_user_defined_suffix"
   */
  def getUserDefinedSuffixKey: Option[String] = {
    connector match {
      case c: FileConnector => Option(c.getUserDefinedSuffixKey)
      case _ => None
    }
  }

  /**
   * Get the storage type
   *
   * @return
   */
  def getStorage: Storage = connector.storage

  /**
   * Set the connector of this spark repository
   *
   * @param connector [[com.jcdecaux.setl.storage.connector.Connector]] an user defined connector
   * @return
   */
  def setConnector(connector: Connector): this.type = {
    this.connector = connector
    flushReadCache.set(true)
    this
  }

  /**
   * Get the connector of this repository
   *
   * @return a [[com.jcdecaux.setl.storage.connector.Connector]] object
   */
  def getConnector: Connector = this.connector

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   * <ul>
   * <li>year=2016/month=01/</li>
   * <li>year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON)
   */
  def partitionBy(columns: String*): this.type = {
    connector match {
      case c: CanPartition => c.partitionBy(columns: _*)
      case _ => throw new InvalidConnectorException("Current connector doesn't support partition")
    }
    this
  }

  private[this] def findDataFrameBy(conditions: Set[Condition]): DataFrame = {
    import com.jcdecaux.setl.util.FilterImplicits.ConditionsToRequest

    if (conditions.nonEmpty) {
      val sql = conditions.toSqlRequest
      logDebug(s"Spark SQL request: $sql")
      connector.read().filter(sql)
    } else {
      connector.read()
    }
  }

  /**
   * Find data that match the given condition set
   *
   * @param conditions Set of [[com.jcdecaux.setl.storage.Condition]]
   * @return
   */
  override def findBy(conditions: Set[Condition]): Dataset[DataType] = {
    val data = readDataFrame(SparkRepository.handleConditions(conditions, schema))
    SchemaConverter.fromDF[DataType](data)
  }

  /**
   * Retrieve all data
   */
  override def findAll(): Dataset[DataType] = {
    SchemaConverter.fromDF[DataType](readDataFrame())
  }

  /**
   * Load data into a DataFrame
   *
   * @param conditions : condition set, by default empty
   * @return
   */
  private[repository] def readDataFrame(conditions: Set[Condition] = Set.empty): DataFrame = {
    if (cacheLastReadData.get()) {
      lock.lock()
      logDebug("Acquire thread lock")
      val thisReadHashCode = conditions.hashCode
      val flush = flushReadCache.getAndSet(false)
      val sameHash = lastReadHashCode.getAndSet(thisReadHashCode) == thisReadHashCode

      try {
        if (!flush && sameHash) {
          logDebug("Load data from read cache")
          readCache
        } else {
          logDebug("Load and cache data")
          if (readCache != null) readCache.unpersist()
          readCache = findDataFrameBy(conditions)
          readCache.persist(persistenceStorageLevel)
          readCache
        }
      } finally {
        lock.unlock()
      }
    } else {
      logDebug("No read cache found, load from data storage")
      findDataFrameBy(conditions)
    }
  }

  /**
   * Write data frame and set flushReadCach to true
   *
   * @param data data to be saved
   */
  private[repository] def writeDataFrame(data: DataFrame): Unit = {
    connector.write(data)
    flushReadCache.set(true)
  }

  /**
   * Save a [[Dataset]] into a data persistence store
   *
   * @param data data to be saved
   */
  override def save(data: Dataset[DataType], suffix: Option[String] = None): SparkRepository.this.type = {

    val dataToSave = SchemaConverter.toDF[DataType](data)

    configureConnector(dataToSave, suffix)
    writeDataFrame(dataToSave)
    this
  }

  private[repository] def configureConnector(df: DataFrame, suffix: Option[String]): Unit = {
    connector match {
      case db: CanCreate =>
        db.create(df)
      case file: FileConnector =>
        file.setSuffix(suffix)
      case _: Connector =>
      case _ =>
        throw new UnknownException.Storage(s"Unknown connector ${connector.getClass.toString}")
    }
  }

  /**
   * Update/Insert a [[Dataset]] into a data persistence store
   *
   * @param data data to be saved
   * @return
   */
  override def update(data: Dataset[DataType]): SparkRepository.this.type = {
    connector match {
      case _: CanUpdate =>
        val dataToSave = SchemaConverter.toDF[DataType](data)
        val primaryColumns = StructAnalyser.findCompoundColumns[DataType]
        if (primaryColumns.nonEmpty)
          updateDataFrame(dataToSave, primaryColumns: _*)
        else {
          logWarning(s"Current Dataset doesn't contain any compound key! Normal write operation will do used.")
          writeDataFrame(dataToSave)
        }
      case _ =>
        throw new InvalidConnectorException(s"Current connector doesn't support update operation!")
    }

    this
  }

  /**
   * Update data frame and set flushReadCach to true
   *
   * @param data data to be saved
   */
  private[repository] def updateDataFrame(data: DataFrame, columns: String*): Unit = {
    connector.asInstanceOf[CanUpdate].update(data, columns: _*)
    flushReadCache.set(true)
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanWait
   */
  override def awaitTermination(): Unit = {
    connector match {
      case c: CanWait => c.awaitTermination()
      case _ => throw new InvalidConnectorException("Current connector doesn't support awaitTermination")
    }
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @param timeout time to wait in milliseconds
   * @return `true` if it's stopped; or throw the reported error during the execution; or `false`
   *         if the waiting time elapsed before returning from the method.
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanWait
   */
  override def awaitTerminationOrTimeout(timeout: Long): Boolean = {
    connector match {
      case c: CanWait => c.awaitTerminationOrTimeout(timeout)
      case _ => throw new InvalidConnectorException("Current connector doesn't support awaitTerminationOrTimeout")
    }
  }

  /**
   * Stops the execution of this query if it is running.
   *
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanWait
   */
  override def stopStreaming(): this.type = {
    connector match {
      case c: CanWait => c.stop()
      case _ => throw new InvalidConnectorException("Current connector doesn't support stop")
    }
    this
  }

  /**
   * Drop the entire table/directory.
   *
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanDrop
   */
  override def drop(): SparkRepository.this.type = {
    connector match {
      case c: CanDrop => c.drop()
      case _ => throw new InvalidConnectorException("Current connector doesn't support drop")
    }
    this
  }

  /**
   * Delete rows according to the query
   *
   * @param query a query string
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanDelete
   */
  override def delete(query: String): SparkRepository.this.type = {
    connector match {
      case c: CanDelete => c.delete(query: String)
      case _ => throw new InvalidConnectorException("Current connector doesn't support drop")
    }
    this
  }

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system) with a suffix
   *
   * @param t      data frame to be written
   * @param suffix suffix to be appended at the end of the data storage name
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait create
   */
  override def create(t: DataFrame, suffix: Option[String]): SparkRepository.this.type = {
    connector match {
      case c: CanCreate => c.create(t, suffix)
      case _ => throw new InvalidConnectorException("Current connector doesn't support create")
    }
    this
  }

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system)
   *
   * @param t data frame to be written
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait create
   */
  override def create(t: DataFrame): SparkRepository.this.type = {
    connector match {
      case c: CanCreate => c.create(t)
      case _ => throw new InvalidConnectorException("Current connector doesn't support create")
    }
    this
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanVacuum
   */
  override def vacuum(retentionHours: Double): SparkRepository.this.type = {
    connector match {
      case c: CanVacuum => c.vacuum(retentionHours)
      case _ => throw new InvalidConnectorException("Current connector doesn't support vacuum")
    }
    this
  }

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * note: This will use the default retention period of 7 days.
   *
   * @throws InvalidConnectorException this exception will be thrown if the current connector doesn't inherit the trait CanVacuum
   */
  override def vacuum(): SparkRepository.this.type = {
    connector match {
      case c: CanVacuum => c.vacuum()
      case _ => throw new InvalidConnectorException("Current connector doesn't support vacuum")
    }
    this
  }
}

object SparkRepository {

  def apply[T: TypeTag]: SparkRepository[T] = new SparkRepository[T]

  /**
   * Change the column name according to DataType schema's annotation (@ColumnName)
   *
   * In the case where a case class field is annotated by @ColumnName, if the name of case class' field is used in the
   * condition, we replace it with its alias (the value given by @ColumnName annotation)
   *
   * @param conditions conditions
   * @return
   */
  private[repository] def handleConditions(conditions: Set[Condition], schema: StructType): Set[Condition] = {

    val columnWithAlias = schema.filter(_.metadata.contains(SchemaConverter.COLUMN_NAME))
    val binaryColumns = schema.filter(_.metadata.contains(SchemaConverter.COMPRESS))


    val binaryColumnNames = binaryColumns.map(_.name)
    val aliasBinaryColumns = binaryColumns
      .filter(bc => columnWithAlias.map(_.name).contains(bc.name))
      .map(bc => bc.metadata.getStringArray(SchemaConverter.COLUMN_NAME).head)

    conditions
      .map {
        cond =>

          cond.valueType match {
            case ValueType.COLUMN =>
              var sqlString = cond.value.get

              // Check if use is trying to filter an binary column
              (binaryColumnNames ++ aliasBinaryColumns).toSet.foreach {
                colName: String =>
                  if (sqlString.contains(s"`$colName`")) {
                    throw new IllegalArgumentException(s"Binary column ${cond.key} couldn't be filtered")
                  }
              }

              /*
              If the current condition is of value type column,
              then we try replacing columns that have alias name by their alias name
              */
              columnWithAlias.foreach {
                col =>
                  val alias = col.metadata.getStringArray(SchemaConverter.COLUMN_NAME).headOption
                  if (alias.nonEmpty) {
                    sqlString = sqlString.replace(s"`${col.name}`", s"`${alias.get}`")
                  }
              }
              cond.copy(value = Option(sqlString))

            case _ =>
              // Check if use is trying to filter an binary column
              if (binaryColumnNames.contains(cond.key) || aliasBinaryColumns.contains(cond.key)) {
                throw new IllegalArgumentException(s"Binary column ${cond.key} couldn't be filtered")
              }

              /* if the current query column has an alias, we recreate a new condition and replace
              the current key by the column name alias
              */
              columnWithAlias.find(_.name == cond.key) match {
                case Some(a) =>
                  cond.copy(key = a.metadata.getStringArray(SchemaConverter.COLUMN_NAME).head)
                case _ => cond
              }
          }


      }
  }

}
