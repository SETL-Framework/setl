package com.jcdecaux.datacorp.spark.storage

import org.apache.spark.sql.{Dataset, Encoder}

/**
  * The goal of Repository is to significantly reduce the amount of boilerplate code required to
  * implement data access layers for various persistence stores.
  *
  * @tparam T
  */
@deprecated("the old repository interface is deprecated and will be removed from v0.3", "v0.2.0")
trait Repository[T] {

  def findBy(filters: Set[Filter], suffix: String)(implicit encoder: Encoder[T]): Dataset[T]

  def findBy(filters: Filter, suffix: String)(implicit encoder: Encoder[T]): Dataset[T]

  def findAll(suffix: String)(implicit encoder: Encoder[T]): Dataset[T]

  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type
}
