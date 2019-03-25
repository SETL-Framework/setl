package com.jcdecaux.datacorp.spark.storage.cassandra

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}
import com.jcdecaux.datacorp.spark.storage.{Filter, Repository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils

/**
  * SparkCassandraRepository
  *
  * @tparam T
  */
trait CassandraSparkRepository[T] extends Repository[T] with CassandraConnector {

  /**
    *
    * @param encoder
    * @return
    */
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readCassandra()
      .as[T]
  }

  /**
    *
    * @param filters
    * @param encoder
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
    *
    * @param filter
    * @param encoder
    * @return
    */
  def findBy(filter: Filter)(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readCassandra()
      .filter(SqlExpressionUtils.request(filter))
      .as[T]
  }

  /**
    *
    * @param data
    * @param encoder
    * @return
    */
  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type = {
    this.createCassandra(data.toDF())
    this.writeCassandra(data.toDF())
    this
  }
}
