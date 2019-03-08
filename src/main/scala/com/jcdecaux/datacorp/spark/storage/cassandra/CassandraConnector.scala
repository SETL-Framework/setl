package com.jcdecaux.datacorp.spark.storage.cassandra

import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.spark.connector._

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

trait CassandraConnector {

  private[this] final val logger: Logger = Logger.getLogger(this.getClass)

  val keyspace: String
  val table: String
  val spark: SparkSession
  val partitionKeyColumns: Option[Seq[String]]
  val clusteringKeyColumns: Option[Seq[String]]

  /**
    * Read a cassandra table
    *
    * @param spark    spark session
    * @param table    table name
    * @param keyspace keyspace name
    * @return
    */
  private[this] def readCassandra(spark: SparkSession, table: String, keyspace: String): DataFrame = {
    logger.debug(s"Read $keyspace.$table")
    spark.read.cassandraFormat(table, keyspace).load()
  }

  /**
    * Read a cassandra table
    *
    * @return
    */
  protected def readCassandra(): DataFrame = {
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
    logger.debug(s"Write DataFrame to $keyspace.$table")
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
  protected def writeCassandra(df: DataFrame): Unit = {
    this.writeCassandra(df, this.table, this.keyspace)
  }

  /**
    * Create a Cassandra table for a DataFrame
    *
    * @param df DataFrame that will be used to create Cassandra table
    */
  protected def createCassandra(df: DataFrame): Unit = {
    logger.debug(s"Create cassandra table $keyspace.$table")
    logger.debug(s"Partition keys: ${partitionKeyColumns.get.mkString(", ")}")
    logger.debug(s"Clustering keys: ${clusteringKeyColumns.get.mkString(", ")}")
    try {
      df.createCassandraTable(keyspace, table, partitionKeyColumns, clusteringKeyColumns)
    } catch {
      case _: AlreadyExistsException => logger.warn(s"Table $keyspace.$table already exist, append data to it")
    }
  }

  /**
    * Delete a record
    *
    * @param spark    spark session
    * @param table    table name
    * @param query    query
    * @param keyspace keyspace name
    */
  protected def deleteCassandra(query: String = "", keyspace: String): Unit = {
    spark.sparkContext.cassandraTable(keyspace, table)
      .where(query)
      .deleteFromCassandra(keyspace, table)
  }
}
