package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.{MockCassandra, TestTraffic}
import org.apache.spark.sql.SparkSession

class TestTrafficRepository(override val spark: SparkSession) extends SparkRepository[TestTraffic] {
  override val storage: Storage = Storage.CASSANDRA
  override val path: String = null
  override val table: String = "traffic"
  override val keyspace: String = MockCassandra.keyspace
  override val partitionKeyColumns: Option[Seq[String]] = Some(Seq("country", "location", "asset"))
  override val clusteringKeyColumns: Option[Seq[String]] = Some(Seq("datetime"))
}