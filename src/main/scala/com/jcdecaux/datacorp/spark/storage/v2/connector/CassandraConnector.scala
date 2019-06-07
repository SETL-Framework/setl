package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.spark.connector._
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CassandraConnector establish the connection to a given cassandra table of a given keyspace
  */
class CassandraConnector(val keyspace: String,
                         val table: String,
                         val spark: SparkSession,
                         val partitionKeyColumns: Option[Seq[String]],
                         val clusteringKeyColumns: Option[Seq[String]]) extends EnrichConnector with Logging {

  def this(spark: SparkSession, config: Config) = this(
    keyspace = ConfigUtils.getAs[String](config, "keyspace").get,
    table = ConfigUtils.getAs[String](config, "table").get,
    spark = spark,
    partitionKeyColumns =
      Option(ConfigUtils.getList(config, "partitionKeyColumns").get.map(_.toString)),
    clusteringKeyColumns =
      if (ConfigUtils.isDefined(config, "clusteringKeyColumns")) {
        Option(ConfigUtils.getList(config, "clusteringKeyColumns").get.map(_.toString))
      } else {
        None
      }
  )


  /**
    * Read a cassandra table
    *
    * @param spark    spark session
    * @param table    table name
    * @param keyspace keyspace name
    * @return
    */
  private[this] def readCassandra(spark: SparkSession, table: String, keyspace: String): DataFrame = {
    log.debug(s"Read $keyspace.$table")
    spark.read.cassandraFormat(table, keyspace).load()
  }

  /**
    * Read a cassandra table
    *
    * @return
    */
  override def read(): DataFrame = {
    this.readCassandra(this.spark, this.table, this.keyspace)
  }

  /**
    * Write a DataFrame into a Cassandra table
    *
    * @param df       DataFrame to be saved
    * @param table    table name
    * @param keyspace keyspace name
    */
  private[this] def writeCassandra(df: DataFrame, table: String, keyspace: String): Unit = {
    log.debug(s"Write DataFrame to $keyspace.$table")
    df.write
      .cassandraFormat(table, keyspace)
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * Write a DataFrame into a Cassandra table
    *
    * @param df DataFrame to be saved
    */
  override def write(df: DataFrame): Unit = {
    this.writeCassandra(df, this.table, this.keyspace)
  }

  /**
    * Create a Cassandra table for a DataFrame
    *
    * @param df DataFrame that will be used to create Cassandra table
    */
  override def create(df: DataFrame): Unit = {
    log.debug(s"Create cassandra table $keyspace.$table")
    log.debug(s"Partition keys: ${partitionKeyColumns.get.mkString(", ")}")
    log.debug(s"Clustering keys: ${clusteringKeyColumns.getOrElse(Seq("None")).mkString(", ")}")
    try {
      df.createCassandraTable(keyspace, table, partitionKeyColumns, clusteringKeyColumns)
    } catch {
      case _: AlreadyExistsException => log.warn(s"Table $keyspace.$table already exist, append data to it")
    }
  }

  /**
    * Delete a record
    *
    * @param query    query string
    * @param keyspace keyspace name
    */
  private[this] def deleteCassandra(query: String, table: String, keyspace: String): Unit = {
    spark.sparkContext.cassandraTable(keyspace, table)
      .where(query)
      .deleteFromCassandra(keyspace, table)
  }

  /**
    * Delete a record
    *
    * @param query query string
    */
  override def delete(query: String): Unit = {
    this.deleteCassandra(query, this.table, this.keyspace)
  }
}
