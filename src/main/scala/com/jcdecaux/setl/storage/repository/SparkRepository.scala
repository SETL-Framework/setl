package com.jcdecaux.setl.storage.repository

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import com.jcdecaux.setl.annotation.{ColumnName, Compress, InterfaceStability}
import com.jcdecaux.setl.enums.{Storage, ValueType}
import com.jcdecaux.setl.exception.UnknownException
import com.jcdecaux.setl.internal.{Logging, SchemaConverter, StructAnalyser}
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.connector.{Connector, DBConnector, FileConnector}
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
class SparkRepository[DataType: TypeTag] extends Repository[Dataset[DataType]] with Logging {

  private[this] var connector: Connector = _
  private[this] implicit val dataEncoder: Encoder[DataType] = ExpressionEncoder[DataType]
  private[this] val schema: StructType = StructAnalyser.analyseSchema[DataType]
  private[this] val lock: ReentrantLock = new ReentrantLock()

  private[this] val cacheLastReadData: AtomicBoolean = new AtomicBoolean(false)
  private[this] val flushReadCache: AtomicBoolean = new AtomicBoolean(true)
  private[this] val lastReadHashCode: AtomicInteger = new AtomicInteger(0)
  private[this] var persistenceStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  private[this] var readCache: DataFrame = _

  def persistReadData: Boolean = this.cacheLastReadData.get()

  def persistReadData(persist: Boolean): this.type = {
    this.cacheLastReadData.set(persist)
    this
  }

  def getReadCacheStorageLevel: StorageLevel = this.persistenceStorageLevel

  def setReadCacheStorageLevel(storageLevel: StorageLevel): this.type = {
    this.persistenceStorageLevel = storageLevel
    this
  }

  def setUserDefinedSuffixKey(key: String): this.type = {
    connector match {
      case c: FileConnector => c.setUserDefinedSuffixKey(key)
      case _ => log.warn(s"Current connector doesn't support user defined suffix, skip UDS setting")
    }
    this
  }

  def getUserDefinedSuffixKey: Option[String] = {
    connector match {
      case c: FileConnector => Option(c.getUserDefinedSuffixKey)
      case _ => None
    }
  }

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

  def getConnector: Connector = this.connector

  def partitionBy(columns: String*): this.type = {
    connector match {
      case c: FileConnector => c.partitionBy(columns: _*)
      case _ =>
    }
    this
  }

  private[this] def findDataFrameBy(conditions: Set[Condition]): DataFrame = {
    import com.jcdecaux.setl.util.FilterImplicits.ConditionsToRequest

    if (conditions.nonEmpty) {
      val sql = conditions.toSqlRequest
      log.debug(s"Spark SQL request: $sql")
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
      log.debug("Acquire thread lock")
      val thisReadHashCode = conditions.hashCode
      val flush = flushReadCache.getAndSet(false)
      val sameHash = lastReadHashCode.getAndSet(thisReadHashCode) == thisReadHashCode

      try {
        if (!flush && sameHash) {
          log.debug("Load data from read cache")
          readCache
        } else {
          log.debug("Load and cache data")
          if (readCache != null) readCache.unpersist()
          readCache = findDataFrameBy(conditions)
          readCache.persist(persistenceStorageLevel)
          readCache
        }
      } finally {
        lock.unlock()
      }
    } else {
      log.debug("No read cache found, load from data storage")
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
      case db: DBConnector =>
        db.create(df)
      case file: FileConnector =>
        file.setSuffix(suffix)
      case _: Connector =>
      case _ =>
        throw new UnknownException.Storage(s"Unknown connector ${connector.getClass.toString}")
    }
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
