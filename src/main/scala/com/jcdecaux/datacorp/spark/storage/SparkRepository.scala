package com.jcdecaux.datacorp.spark.storage

import org.apache.spark.sql.{Dataset, Encoder}

trait SparkRepository[T] {

  def findAll()(implicit encoder: Encoder[T]): Dataset[T]

  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type
}
