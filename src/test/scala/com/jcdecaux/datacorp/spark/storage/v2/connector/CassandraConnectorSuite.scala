package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.datastax.spark.connector.cql.{CassandraConnector => CC}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CassandraConnectorSuite extends FunSuite with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {

  val keyspace = "test_space"
  val spark: SparkSession = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()

  val cqlConnector = new CassandraConnector(
    keyspace = keyspace,
    table = "test_spark_connector",
    spark = spark,
    partitionKeyColumns = Some(Seq("partition1", "partition2")),
    clusteringKeyColumns = Some(Seq("clustering1"))
  )

  override def clearCache(): Unit = CC.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CC(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(connector, "test_space")
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
    cqlConnector.create(testTable.toDF())
    cqlConnector.write(testTable.toDF())

    // read table
    val readTable = cqlConnector.read()
    readTable.show()
    assert(readTable.count() === 3)

    // delete row
    cqlConnector.delete("partition1 = 1 and partition2 = 'p1'")
    assert(cqlConnector.read().count() === 2)
  }
}
