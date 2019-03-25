package com.jcdecaux.datacorp.spark.storage

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

trait Repository[T] {

  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T]

  def findBy(filters: Filter)(implicit encoder: Encoder[T]): Dataset[T]

  def findAll()(implicit encoder: Encoder[T]): Dataset[T]

  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type
}
