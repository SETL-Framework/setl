package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.storage.{Condition, DatasetConverter}
import org.apache.spark.sql.Dataset

/**
  * RepositoryAdapter could be used when one wants to save a `Dataset[A]` to a data store of type `B`.
  *
  * A `Repository[A]` and a `DatasetConverter[A, B]` must be provided (either explicitly or implicitly)
  *
  * @tparam A Type of the Repository
  * @tparam B Target data store type
  */
trait RepositoryAdapter[A, B] {

  val repository: Repository[A]

  val converter: DatasetConverter[A, B]

  def findAllAndConvert(): Dataset[A]

  def findByAndConvert(conditions: Set[Condition]): Dataset[A]

  def findByAndConvert(condition: Condition): Dataset[A]

  def convertAndSave(data: Dataset[A], suffix: Option[String]): this.type

}
