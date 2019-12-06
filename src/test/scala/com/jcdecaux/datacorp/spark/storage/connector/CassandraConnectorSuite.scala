package com.jcdecaux.datacorp.spark.storage.connector

import com.datastax.spark.connector.cql.{CassandraConnector => CC}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CassandraConnectorSuite extends AnyFunSuite with EmbeddedCassandra with BeforeAndAfterAll {


  import SparkTemplate.defaultConf

  override def clearCache(): Unit = CC.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  //  useSparkConf(defaultConf)
  val connector = CC(defaultConf)

  val keyspace = "test_space"

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(connector, "test_space")
      .generateKeyspace()
      .generateCountry("countries")
  }

  test("Manipulate cassandra table") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      //    .setCassandraHost("localhost")
      .build().get()

    val cqlConnector = new CassandraConnector(
      keyspace = keyspace,
      table = "test_spark_connector",
      spark = spark,
      partitionKeyColumns = Some(Seq("partition1", "partition2")),
      clusteringKeyColumns = Some(Seq("clustering1"))
    )

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

  test("Test with auxiliary cassandra connector constructor") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      //    .setCassandraHost("localhost")
      .build().get()

    val cqlConnector = new CassandraConnector(
      keyspace = keyspace,
      table = "test_spark_connector",
      spark = spark,
      partitionKeyColumns = Some(Seq("partition1", "partition2")),
      clusteringKeyColumns = Some(Seq("clustering1"))
    )

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val connector = new CassandraConnector(spark, Properties.cassandraConfig)

    assert(connector.partitionKeyColumns === Option(Seq("partition1", "partition2")))
    assert(connector.clusteringKeyColumns === Option(Seq("clustering1")))

    // Create table and write data
    connector.create(testTable.toDF())
    connector.write(testTable.toDF())

    // read table
    val readTable = connector.read()
    readTable.show()
    assert(readTable.count() === 3)

    // delete row
    connector.delete("partition1 = 1 and partition2 = 'p1'")
    assert(connector.read().count() === 2)
  }
}
