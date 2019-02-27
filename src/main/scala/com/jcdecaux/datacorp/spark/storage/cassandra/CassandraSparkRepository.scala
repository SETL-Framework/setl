package com.jcdecaux.datacorp.spark.storage.cassandra

import org.apache.spark.sql.{Dataset, Encoder}
import com.jcdecaux.datacorp.spark.storage.{Filter, SparkRepository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils

/**
  * SparkCassandraRepository
  *
  * @tparam T
  */
trait CassandraSparkRepository[T] extends SparkRepository[T] with CassandraConnector {

  /**
    *
    * @param encoder
    * @return
    */
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .read()
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
        .read()
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
      .read()
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
    this.create(data.toDF())
    this.write(data.toDF())
    this
  }
}
