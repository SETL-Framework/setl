package com.jcdecaux.datacorp.spark.storage.csv

import com.jcdecaux.datacorp.spark.storage.{Filter, SparkRepository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils
import org.apache.spark.sql.{Dataset, Encoder}


/**
  * SparkCassandraRepository
  *
  * @tparam T
  */
trait CSVSparkRepository[T] extends SparkRepository[T] with CSVConnector {

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
    this.write(data.toDF())
    this
  }
}
