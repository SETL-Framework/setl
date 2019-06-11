package com.jcdecaux.datacorp.spark.storage

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * The goal of Repository is to significantly reduce the amount of boilerplate code required to
  * implement data access layers for various persistence stores.
  *
  * @tparam T
  */
trait Repository[T] {

  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T]

  def findBy(filters: Filter)(implicit encoder: Encoder[T]): Dataset[T]

  def findAll()(implicit encoder: Encoder[T]): Dataset[T]

  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type
}
