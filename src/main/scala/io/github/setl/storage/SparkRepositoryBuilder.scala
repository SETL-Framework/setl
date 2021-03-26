package io.github.setl.storage

import io.github.setl.Builder
import io.github.setl.annotation.InterfaceStability
import io.github.setl.config.Conf
import io.github.setl.config.Conf.Serializer
import io.github.setl.enums.Storage
import io.github.setl.storage.connector._
import io.github.setl.storage.repository.SparkRepository
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.{universe => ru}

/**
 * The SparkRepositoryBuilder will build a [[SparkRepository]] according to the given [[DataType]] and [[Storage]]
 *
 * @param storage type of storage
 * @param config  a [[com.typesafe.config.Config]] object
 * @tparam DataType type of data
 */
@InterfaceStability.Evolving
class SparkRepositoryBuilder[DataType: ru.TypeTag](var storage: Option[Storage], var config: Option[Config])
  extends Builder[SparkRepository[DataType]] {

  def this() = this(None, None)

  def this(storage: Storage) = this(Some(storage), None)

  def this(config: Config) = this(None, Some(config))

  import Conf.Serializer._

  private[this] val conf: Conf = new Conf()

  if (config.isEmpty) {

    storage match {
      case Some(s) => set("storage", s)
      case _ =>
    }
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
  private[this] var sparkRepository: io.github.setl.storage.repository.SparkRepository[DataType] = _

  def set[T](key: String, value: T)(implicit converter: Serializer[T]): this.type = {
    conf.set(key, value)
    this
  }

  def getAs[T](key: String)(implicit converter: Serializer[T]): Option[T] = {
    conf.getAs[T](key)
  }

  def setStorage(storage: Storage): this.type = set("storage", storage)

  def setKeyspace(keyspace: String): this.type = set("keyspace", keyspace)

  def setTable(table: String): this.type = set("table", table)

  def setPartitionKeys(cols: Option[Seq[String]]): this.type = set("partitionKeyColumns", cols.get.toArray)

  def setClusteringKeys(cols: Option[Seq[String]]): this.type = set("clusteringKeyColumns", cols.get.toArray)

  def setPath(path: String): this.type = set("path", path)

  def setInferSchema(boo: Boolean): this.type = set("inferSchema", boo)

  def setSchema(schema: StructType): this.type = {

    // For spark version < 2.4, there was no method toDDL in StructType.
    val structDDL = try {
      val method = schema.getClass.getDeclaredMethod("toDDL")
      method.invoke(schema).toString
    } catch {
      case _: java.lang.NoSuchMethodException =>
        schema.map(sf => s"${sf.name} ${sf.dataType.sql}").mkString(", ")
      case e: Throwable => throw e
    }

    set("schema", structDDL)
  }

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

  def setCustomConnectorClass(cls: String): this.type = set(ConnectorBuilder.CLASS, cls)

  /**
   * Build an object
   *
   * @return
   */
  override def build(): SparkRepositoryBuilder.this.type = {
    logDebug(s"Build SparkRepository[${ru.typeOf[DataType]}]")
    if (connector == null) {
      connector = createConnector()
    }
    sparkRepository = new io.github.setl.storage.repository.SparkRepository[DataType].setConnector(connector)
    this
  }

  /**
   * Create the connector according to the storage type
   *
   * @return [[Connector]]
   */
  protected[this] def createConnector(): Connector = {
    // if a TypeSafe config is set, then return a corresponding connector
    config match {
      case Some(typeSafeConfig) =>
        logDebug("Build connector with TypeSafe configuration")
        return new ConnectorBuilder(typeSafeConfig).build().get()
      case _ =>
    }

    // Otherwise, build a connector according to the current configuration
    logDebug("No TypeSafe configuration was found, build with parameters")
    new ConnectorBuilder(conf)
      .build()
      .get()

  }

  def setConnector(connector: Connector): this.type = {
    logInfo(s"Set user-defined ${connector.getClass} connector")
    this.connector = connector
    this
  }

  /**
   * Get the built spark repository
   *
   * @return [[SparkRepository]]
   */
  override def get(): io.github.setl.storage.repository.SparkRepository[DataType] = this.sparkRepository
}
