package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.TestObject
import com.jcdecaux.datacorp.spark.enums.Storage
import org.apache.spark.sql.SparkSession

class TestObjectParquetSparkRepository(override val spark: SparkSession) extends SparkRepository[TestObject] {
  override val storage: Storage = Storage.PARQUET
  override val path: String = "src/test/resources/test_spark_repo_parquet"
  override val table: String = "test_spark_repo_parquet"
  override val keyspace: String = null
  override val partitionKeyColumns: Option[Seq[String]] = None
  override val clusteringKeyColumns: Option[Seq[String]] = None

}
