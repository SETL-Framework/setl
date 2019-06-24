package com.jcdecaux.datacorp.spark.storage.cassandra

import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.spark.connector._
import com.jcdecaux.datacorp.spark.internal.Logging
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * CassandraConnector establish the connection to a given cassandra table of a given keyspace
  */
@deprecated("the old connector interface is deprecated and will be removed from v0.3", "v0.2.0")
trait CassandraConnector extends Logging {

  val keyspace: String
  val table: String
  val spark: SparkSession
  val partitionKeyColumns: Option[Seq[String]]
  val clusteringKeyColumns: Option[Seq[String]]

  /**
    * Read a cassandra table
    *
    * @return
    */
  protected def readCassandra(): DataFrame = {
    this.readCassandra(this.spark, this.table, this.keyspace)
  }

  /**
    * Read a cassandra table
    *
    * @param spark    spark session
    * @param table    table name
    * @param keyspace keyspace name
    * @return
    */
  private[this] def readCassandra(spark: SparkSession, table: String, keyspace: String): DataFrame = {
    log.debug(s"Read Cassandra table $keyspace.$table")
    spark.read.cassandraFormat(table, keyspace).load()
  }

  /**
    * Write a DataFrame into a Cassandra table
    *
    * @param df DataFrame to be saved
    */
  protected def writeCassandra(df: DataFrame): Unit = {
    this.writeCassandra(df, this.table, this.keyspace)
  }

  /**
    * Write a DataFrame into a Cassandra table
    *
    * @param df       DataFrame to be saved
    * @param table    table name
    * @param keyspace keyspace name
    */
  private[this] def writeCassandra(df: DataFrame, table: String, keyspace: String): Unit = {
    log.debug(s"Write DataFrame to Cassandra table $keyspace.$table")
    df.write
      .cassandraFormat(table, keyspace)
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * Create a Cassandra table for a DataFrame
    *
    * @param df DataFrame that will be used to create Cassandra table
    */
  protected def createCassandra(df: DataFrame): Unit = {
    log.debug(s"Create cassandra table $keyspace.$table")
    log.debug(s"Partition keys: ${partitionKeyColumns.get.mkString(", ")}")
    log.debug(s"Clustering keys: ${clusteringKeyColumns.get.mkString(", ")}")
    try {
      df.createCassandraTable(keyspace, table, partitionKeyColumns, clusteringKeyColumns)
    } catch {
      case _: AlreadyExistsException => log.warn(s"Table $keyspace.$table already exist, append data to it")
    }
  }

  /**
    * Delete a record
    *
    * @param query query string
    */
  protected def deleteCassandra(query: String): Unit = {
    this.deleteCassandra(query, this.table, this.keyspace)
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
}
