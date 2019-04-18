package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.{MockCassandra, TestObject}
import org.apache.spark.sql.SparkSession

class TestObjectCassandraSparkRepository(override val spark: SparkSession) extends SparkRepository[TestObject] {
  override val storage: Storage = Storage.CASSANDRA
  override val path: String = null
  override val table: String = "test_cassandra_repo"
  override val keyspace: String = MockCassandra.keyspace
  override val partitionKeyColumns: Option[Seq[String]] = Some(Seq("partition1", "partition2"))
  override val clusteringKeyColumns: Option[Seq[String]] = Some(Seq("clustering1"))
}
