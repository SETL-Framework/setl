package com.jcdecaux.datacorp.spark.storage.csv

import com.jcdecaux.datacorp.spark.storage.{Filter, Repository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

/**
  * SparkCassandraRepository
  *
  * @tparam T
  */
trait CSVSparkRepository[T] extends Repository[T] with CSVConnector {

  /**
    *
    * @param encoder
    * @return
    */
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readCSV()
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
        .readCSV()
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
      .readCSV()
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
    this.save(data, SaveMode.Overwrite)
    this
  }

  /**
    *
    * @param data
    * @param encoder
    * @return
    */
  def save(data: Dataset[T], saveMode: SaveMode)(implicit encoder: Encoder[T]): this.type = {
    this.writeCSV(data.toDF(), saveMode)
    this
  }
}
