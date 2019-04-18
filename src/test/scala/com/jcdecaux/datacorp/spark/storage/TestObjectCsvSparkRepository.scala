package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.TestObject
import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql.SparkSession

class TestObjectCsvSparkRepository(override val spark: SparkSession) extends SparkRepository[TestObject] {
  override val storage: Storage = Storage.CSV
  override val path: String = "src/test/resources/test_spark_repo_csv"
  override val table: String = "test_spark_repo_csv"
  override val keyspace: String = null
  override val partitionKeyColumns: Option[Seq[String]] = None
  override val clusteringKeyColumns: Option[Seq[String]] = None
}
