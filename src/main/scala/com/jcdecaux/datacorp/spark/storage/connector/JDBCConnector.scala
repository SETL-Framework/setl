package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.config.JDBCConnectorConf
import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql._

class JDBCConnector(val conf: JDBCConnectorConf) extends DBConnector {

  def this(url: String, table: String, user: String, password: String) = {
    this(
      new JDBCConnectorConf()
        .setUrl(url)
        .setDbTable(table)
        .setUser(user)
        .setPassword(password)
    )
  }

  def this(conf: Map[String, String]) = this(JDBCConnectorConf.fromMap(conf))

  override val storage: Storage = Storage.JDBC

  override val spark: SparkSession = SparkSession.getActiveSession.get

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

  override def delete(query: String): Unit = log.warn("Delete is not supported in JDBC Connector")


  override def read(): DataFrame = reader.load()

  override def write(t: DataFrame, suffix: Option[String]): Unit = write(t)

  override def write(t: DataFrame): Unit = writer(t).save()
}
