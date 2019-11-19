package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.internal.{SchemaConverter, StructAnalyser}
import com.jcdecaux.datacorp.spark.storage.{Condition, DatasetConverter}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

object ImplicitRepositoryAdapter {

  /**
    * SparkRepositoryAdapter is an implemented implicit RepositoryAdapter that provides 4 additional methods to an
    * existing `SparkRepository[A]`.
    *
    * {{{
    *   // Example:
    *
    *   implicit val converter = new DatasetConverter[A, B] {
    *     // implementation
    *   }
    *
    *   val defaultRepository: SparkRepository[A]  // a default repository that can save a Dataset[A]
    *
    *   import com.jcdecaux.datacorp.spark.storage.repository.ImplicitRepositoryAdapter._
    *
    *   // This will convert dsOfTypeA (a Dataset[A]) to a Dataset[B] by using the previous implicit converter, then
    *   // save the converted dataset into the data store
    *   defaultRepository.convertAndSave(dsOfTypeA)
    *
    *   defaultRepository.findAllAndConvert()
    * }}}
    *
    * @param repository an existing repository
    * @param converter  a DatasetConverter (should be implemented by user)
    * @tparam A source type
    * @tparam B target type
    */
  implicit class SparkRepositoryAdapter[A: TypeTag, B: TypeTag]
  (override val repository: SparkRepository[A])
  (override implicit val converter: DatasetConverter[A, B]) extends RepositoryAdapter[A, B] {

    private[this] val DBTypeSchema: StructType = StructAnalyser.analyseSchema[B]

    def findAllAndConvert(): Dataset[A] = {
      val data = repository.readDataFrame()
      converter.convertFrom(SchemaConverter.fromDF[B](data))
    }

    def findByAndConvert(conditions: Set[Condition]): Dataset[A] = {
      val data = repository.readDataFrame(SparkRepository.handleConditions(conditions, DBTypeSchema))
      converter.convertFrom(SchemaConverter.fromDF[B](data))
    }

    def findByAndConvert(condition: Condition): Dataset[A] = {
      findByAndConvert(Set(condition))
    }

    def convertAndSave(data: Dataset[A], suffix: Option[String] = None): SparkRepositoryAdapter.this.type = {
      val dsToSave = converter.convertTo(data)
      repository.configureConnector(dsToSave.toDF(), suffix)
      repository.writeDataFrame(SchemaConverter.toDF[B](dsToSave))
      this
    }
  }

}