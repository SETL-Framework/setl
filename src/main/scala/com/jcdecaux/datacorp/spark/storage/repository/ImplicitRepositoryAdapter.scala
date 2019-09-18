package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.internal.{SchemaConverter, StructAnalyser}
import com.jcdecaux.datacorp.spark.storage.{Condition, DatasetConverter}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

object ImplicitRepositoryAdapter {

  implicit class SparkRepositoryAdapter[GenericType: TypeTag, DBType: TypeTag]
  (override val repository: SparkRepository[GenericType])
  (override implicit val converter: DatasetConverter[GenericType, DBType]) extends RepositoryAdapter[GenericType, DBType] {

    private[this] val DBTypeSchema: StructType = StructAnalyser.analyseSchema[DBType]

    def findAllAndConvert(): Dataset[GenericType] = {
      val data = repository.getConnector.read()
      converter.convertFrom(SchemaConverter.fromDF[DBType](data))
    }

    def findByAndConvert(conditions: Set[Condition]): Dataset[GenericType] = {
      val data = repository.findDataFrameBy(SparkRepository.handleConditions(conditions, DBTypeSchema))
      converter.convertFrom(SchemaConverter.fromDF[DBType](data))
    }

    def findByAndConvert(condition: Condition): Dataset[GenericType] = {
      findByAndConvert(Set(condition))
    }

    def convertAndSave(data: Dataset[GenericType], suffix: Option[String] = None): SparkRepositoryAdapter.this.type = {
      val dsToSave = converter.convertTo(data)
      repository.configureConnector(dsToSave.toDF(), suffix)
      repository.getConnector.write(SchemaConverter.toDF[DBType](dsToSave))
      this
    }
  }

}