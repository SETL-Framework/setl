package com.jcdecaux.setl.storage.connector

import java.util.concurrent.atomic.AtomicBoolean

import com.jcdecaux.setl.config.{Conf, JDBCConnectorConf}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.internal.HasReaderWriter
import com.jcdecaux.setl.util.{SparkUtils, TypesafeConfigUtils}
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc._

class JDBCConnector(val conf: JDBCConnectorConf) extends DBConnector with HasReaderWriter {

  private[this] val _read: AtomicBoolean = new AtomicBoolean(false)
  private[this] val _source: String = s"JDBCRelation(${conf.getDbTable.get})"

  def this(url: String, table: String, user: String, password: String, saveMode: SaveMode = SaveMode.ErrorIfExists) = {
    this(
      new JDBCConnectorConf()
        .setUrl(url)
        .setDbTable(table)
        .setUser(user)
        .setPassword(password)
        .setSaveMode(saveMode)
    )
  }

  def this(conf: Map[String, String]) = this(JDBCConnectorConf.fromMap(conf))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(sparkSession: SparkSession, config: Config) = this(config)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(sparkSession: SparkSession, conf: Conf) = this(conf)

  private[this] def executeRequest(request: String): Unit = {
    val statement = JdbcUtils.createConnectionFactory(conf.getJDBCOptions)().createStatement()
    statement.execute(request)
    statement.close()
  }

  override val storage: Storage = Storage.JDBC

  override val reader: DataFrameReader = {
    val _reader = spark.read
      .format(conf.getFormat.get)
      .options(conf.getReaderConf)

    _reader
  }

  override val writer: DataFrame => DataFrameWriter[Row] = jdbcDF => {

    conf.getWriterConf.foreach(println)
    val _writer = jdbcDF.write
      .format("jdbc")
      .options(conf.getWriterConf)
      .mode(conf.getSaveMode.getOrElse(SaveMode.ErrorIfExists.toString))

    _writer
  }

  override def create(t: DataFrame, suffix: Option[String]): Unit = create(t)

  override def create(t: DataFrame): Unit = {
    log.warn("Create is not supported in JDBC Connector")
  }

  override def delete(condition: String): Unit = {
    this.executeRequest(s"DELETE FROM ${conf.getDbTable.get} WHERE $condition;")
  }

  override def read(): DataFrame = {
    this.setJobDescription(s"Read table ${conf.getDbTable.getOrElse("unknown")}")
    _read.set(true)
    reader.load()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = write(t)

  override def write(t: DataFrame): Unit = {
    this.setJobDescription(s"Write data to table ${conf.getDbTable.getOrElse("unknown")}")

    conf.getSaveMode match {
      case Some(sm) =>
        /*
          If the write mode is overwrite, and the data to be written were loaded from the current connector, and the data
          are not persisted, throw a runtime exception because it will truncate the table but not insert any new data
          (because of the laziness of spark)
       */
        if (sm == SaveMode.Overwrite.toString && this._read.get()) {

          val queryExplainCommand = SparkUtils.explainCommandWithExtendedMode(t.queryExecution.logical)
          val explain = spark.sessionState
            .executePlan(queryExplainCommand)
            .executedPlan
            .executeCollect()
            .map(_.getString(0))
            .mkString("; ")

          if (explain.contains(_source) && // if t comes from the same source of this connector
            !t.storageLevel.useMemory && // if t is not persisted in memory
            !t.storageLevel.useDisk && // if t is not persisted in disk
            !t.storageLevel.useOffHeap) { // if t is not persisted using OffHeap
            throw new RuntimeException("Find save mode equals Overwrite and non-persisted data of the same data source. " +
              "The write operation is stop because it will only truncate the table.")
          }
        }
      case _ => // do nothing if save mode is not overwrite or data is not read
    }

    writer(t).save()

  }

  override def drop(): Unit = {
    this.executeRequest(s"DROP TABLE IF EXISTS ${conf.getDbTable.get};")
  }

}
