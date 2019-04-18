package com.jcdecaux.datacorp.spark.storage.cassandra

import com.jcdecaux.datacorp.spark.storage.{Filter, Repository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils
import org.apache.spark.sql.{Dataset, Encoder}

/**
  * SparkCassandraRepository
  *
  * @tparam T the corresponding Scala class for a Cassandra table
  */
trait CassandraSparkRepository[T] extends Repository[T] with CassandraConnector {

  /**
    * Return all the rows
    *
    * @param encoder an implicit Encoder for converting a Cassandra table object to a Spark SQL representation
    * @return
    */
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readCassandra()
      .as[T]
  }

  /**
    * Find rows matching the given filter conditions
    *
    * @param filters a set of filter conditions
    * @param encoder an implicit Encoder for converting a Cassandra table object to a Spark SQL representation
    * @return
    */
  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T] = {
    if(filters.nonEmpty) {
      this
        .readCassandra()
        .filter(SqlExpressionUtils.build(filters))
        .as[T]
    } else {
      this.findAll()
    }
  }

  /**
    * Find rows matching the given filter condition
    *
    * @param filter  a filter condition
    * @param encoder an implicit Encoder for converting a Cassandra table object to a Spark SQL representation
    * @return
    */
  def findBy(filter: Filter)(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readCassandra()
      .filter(SqlExpressionUtils.request(filter))
      .as[T]
  }

  /**
    * Save a Dataset to Cassandra
    *
    * @param data    Spark Dataset to save
    * @param encoder an implicit Encoder for converting a Cassandra table object to a Spark SQL representation
    * @return
    */
  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type = {
    this.createCassandra(data.toDF())
    this.writeCassandra(data.toDF())
    this
  }
}
