package com.jcdecaux.datacorp.spark.storage.cassandra

import com.datastax.spark.connector.cql.{CassandraConnector => CC}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CassandraConnectorSuite extends FunSuite with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll with CassandraConnector {

  override val keyspace: String = "test_space"
  override val table: String = "test_spark_connector"
  override val spark: SparkSession = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()
  override val partitionKeyColumns: Option[Seq[String]] = Some(Seq("partition1", "partition2"))
  override val clusteringKeyColumns: Option[Seq[String]] = Some(Seq("clustering1"))

  override def clearCache(): Unit = CC.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CC(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(connector, keyspace)
      .generateKeyspace()
      .generateCountry("countries")
  }

  test("Manipulate cassandra table") {
    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    // Create table and write data
    this.createCassandra(testTable.toDF())
    this.writeCassandra(testTable.toDF())

    // read table
    val readTable = this.readCassandra()
    readTable.show()
    assert(readTable.count() === 3)

    // delete row
    this.deleteCassandra("partition1 = 1 and partition2 = 'p1'")
    assert(this.readCassandra().count() === 2)
  }
}
