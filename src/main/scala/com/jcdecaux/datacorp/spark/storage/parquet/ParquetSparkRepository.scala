package com.jcdecaux.datacorp.spark.storage.parquet

import com.jcdecaux.datacorp.spark.exception.SqlExpressionUtils
import com.jcdecaux.datacorp.spark.storage.{Filter, SparkRepository}
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

/**
  * SparkCassandraRepository
  *
  * @tparam T
  */
trait ParquetSparkRepository[T] extends SparkRepository[T] with ParquetConnector {

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
    * @param filters
    * @param encoder
    * @return
    */
  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T] = {
    if(filters.nonEmpty) {
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
  def save(data: Dataset[T], saveMode: SaveMode = SaveMode.Overwrite)(implicit encoder: Encoder[T]): this.type = {
    this.writeParquet(data.toDF(), saveMode)
    this
  }
}
