package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.{MockCassandra, TestDwellTime}
import org.apache.spark.sql.SparkSession

class TestDwellTimeRepository(override val spark: SparkSession) extends SparkRepository[TestDwellTime] {
  override val storage: Storage = Storage.CASSANDRA
  override val path: String = null
  override val table: String = "dwell"
  override val keyspace: String = MockCassandra.keyspace
  override val partitionKeyColumns: Option[Seq[String]] = Some(Seq("country", "location", "asset", "dwell_time"))
  override val clusteringKeyColumns: Option[Seq[String]] = Some(Seq("date"))
}