package com.jcdecaux.datacorp.spark.storage.parquet

import com.jcdecaux.datacorp.spark.storage.{Filter, Repository}
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

/**
  * ParquetSparkRepository
  *
  * @tparam T
  */
trait ParquetSparkRepository[T] extends Repository[T] with ParquetConnector {

  /**
    *
    * @param filters
    * @param encoder
    * @return
    */
  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T] = {
    if (filters.nonEmpty) {
      this
        .readParquet()
        .filter(SqlExpressionUtils.build(filters))
        .as[T]
    } else {
      this.findAll()
    }
  }

  /**
    *
    * @param encoder
    * @return
    */
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readParquet()
      .as[T]
  }

  /**
    *
    * @param filter
    * @param encoder
    * @return
    */
  def findBy(filter: Filter)(implicit encoder: Encoder[T]): Dataset[T] = {
    this
      .readParquet()
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
    this.writeParquet(data.toDF(), saveMode)
    this
  }
}
