package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.storage.{Condition, DatasetConverter}
import org.apache.spark.sql.Dataset

trait RepositoryAdapter[A, B] {

  val repository: Repository[A]

  val converter: DatasetConverter[A, B]

  def findAllAndConvert(): Dataset[A]

  def findByAndConvert(conditions: Set[Condition]): Dataset[A]

  def findByAndConvert(condition: Condition): Dataset[A]

  def convertAndSave(data: Dataset[A], suffix: Option[String]): this.type

}
