package com.jcdecaux.setl.storage.connector

import com.datastax.spark.connector.cql.{CassandraConnector => DatastaxCassandraConnector}
import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.spark.connector._
import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

/**
 * CassandraConnector establish the connection to a given cassandra table of a given keyspace
 */
@InterfaceStability.Evolving
class CassandraConnector(val keyspace: String,
                         val table: String,
                         val partitionKeyColumns: Option[Seq[String]],
                         val clusteringKeyColumns: Option[Seq[String]]) extends DBConnector {

  def this(keyspace: String,
           table: String,
           spark: SparkSession,
           partitionKeyColumns: Option[Seq[String]],
           clusteringKeyColumns: Option[Seq[String]]) = this(keyspace, table, partitionKeyColumns, clusteringKeyColumns)

  /**
   * Constructor with a [[com.jcdecaux.setl.config.Conf]] object
   *
   * @param conf [[com.jcdecaux.setl.config.Conf]] object
   */
  def this(conf: Conf) = this(
    keyspace = conf.get("keyspace").get,
    table = conf.get("table").get,
    partitionKeyColumns = Option(conf.getAs[Array[String]]("partitionKeyColumns").get.toSeq),
    clusteringKeyColumns =
      if (conf.getAs[Array[String]]("clusteringKeyColumns").isDefined) {
        Option(conf.getAs[Array[String]]("clusteringKeyColumns").get.toSeq)
      } else {
        None
      }
  )

  /**
   * Constructor with a [[com.typesafe.config.Config]] object
   *
   * @param config [[com.typesafe.config.Config]] typesafe Config object
   */
  def this(config: Config) = this(
    keyspace = TypesafeConfigUtils.getAs[String](config, "keyspace").get,
    table = TypesafeConfigUtils.getAs[String](config, "table").get,
    partitionKeyColumns =
      Option(TypesafeConfigUtils.getList(config, "partitionKeyColumns").get.map(_.toString)),
    clusteringKeyColumns =
      if (TypesafeConfigUtils.isDefined(config, "clusteringKeyColumns")) {
        Option(TypesafeConfigUtils.getList(config, "clusteringKeyColumns").get.map(_.toString))
      } else {
        None
      }
  )

  /**
   * Constructor with a [[com.jcdecaux.setl.config.Conf]] object
   *
   * @param spark spark session
   * @param conf  [[com.jcdecaux.setl.config.Conf]] object
   */
  def this(spark: SparkSession, conf: Conf) = this(conf)

  /**
   * Constructor with a [[com.typesafe.config.Config]] object
   *
   * @param spark  spark session
   * @param config [[com.typesafe.config.Config]] typesafe Config object
   */
  def this(spark: SparkSession, config: Config) = this(config)

  private[this] val cqlConnection =  DatastaxCassandraConnector(spark.sparkContext.getConf)

  override val reader: DataFrameReader = spark.read.cassandraFormat(table, keyspace)

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write.mode(SaveMode.Append)
  }

  override val storage: Storage = Storage.CASSANDRA

  /**
   * Read a cassandra table
   *
   * @return
   */
  override def read(): DataFrame = {
    log.debug(s"Read $keyspace.$table")
    reader.load()
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
    writer(df)
      .cassandraFormat(table, keyspace)
      .save()
  }

  /**
   * Write a DataFrame into a Cassandra table
   *
   * @param df DataFrame to be saved
   */
  override def write(df: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Suffix will be ignored in CassandraConnector")
    write(df)
  }

  /**
   * Create a Cassandra table for a DataFrame
   *
   * @param df DataFrame that will be used to create Cassandra table
   */
  override def create(df: DataFrame, suffix: Option[String]): Unit = {
    log.warn("Suffix will be ignored in CassandraConnector")
    create(df)
  }


  override def create(df: DataFrame): Unit = {
    log.debug(s"Create cassandra table $keyspace.$table")
    log.debug(s"Partition keys: ${partitionKeyColumns.get.mkString(", ")}")
    log.debug(s"Clustering keys: ${clusteringKeyColumns.getOrElse(Seq("None")).mkString(", ")}")
    try {
      df.createCassandraTable(keyspace, table, partitionKeyColumns, clusteringKeyColumns)
    } catch {
      case _: AlreadyExistsException => log.warn(s"Table $keyspace.$table already exist")
    }
  }

  override def write(df: DataFrame): Unit = {
    this.create(df)
    this.writeCassandra(df, this.table, this.keyspace)
  }

  /**
   * Delete a record
   *
   * @param query query string
   */
  override def delete(query: String): Unit = {
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

  override def drop(): Unit = {
    cqlConnection.withSessionDo {
      session =>
        session.execute(s"DROP TABLE IF EXISTS $keyspace.$table;")
    }
  }

  @throws[com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException]("Make sure the strategy is correct")
  def createKeyspace(strategy: String, replicationFactor: Int): Unit = {
    cqlConnection.withSessionDo {
      session =>
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'$strategy', 'replication_factor':$replicationFactor};")
    }
  }

  private[this] def dropKeyspace(): Unit = {
    cqlConnection.withSessionDo {
      session =>
        session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
    }
  }

}
