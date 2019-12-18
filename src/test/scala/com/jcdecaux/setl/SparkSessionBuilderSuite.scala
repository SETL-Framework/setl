package com.jcdecaux.setl

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, SequentialNestedSuiteExecution}


class SparkSessionBuilderSuite extends AnyFunSuite with BeforeAndAfterAll with SequentialNestedSuiteExecution {

  val connector: CassandraConnector = CassandraConnector(MockCassandra.cassandraConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(connector, MockCassandra.keyspace)
      .dropKeyspace()
      .generateKeyspace()
      .generateCountry("countries")
  }

  test("Default instantiation") {
    val sparkSessionBuilder = new SparkSessionBuilder()
    assert(sparkSessionBuilder.appEnv === "local")
    assert(sparkSessionBuilder.appName === "SparkApplication")
    assert(sparkSessionBuilder.cassandraHost === null)
    assert(sparkSessionBuilder.sparkConf.getClass === classOf[SparkConf])
    assert(sparkSessionBuilder.initialization === true)
    assert(sparkSessionBuilder.spark === null)
    sparkSessionBuilder.build()
    assert(sparkSessionBuilder.spark != null)
  }

  test("Cassandra connection") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .build()
      .get()

    assert(spark.read.cassandraFormat("countries", MockCassandra.keyspace).load().count() === 20)
  }

  test("Custom configuration") {

    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.cassandra.connection.keep_alive_ms", "5000")
      .set("spark.cassandra.connection.timeout_ms", "30000")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.ui.enabled", "false")
      .set("spark.cleaner.ttl", "3600")
      .set("spark.cassandra.connection.host", MockCassandra.host)
      .set("myProperty", "hehehe")
      .setAppName("CustomConfigApp")
      .setMaster("local[*]")

    val spark = new SparkSessionBuilder()
      .withSparkConf(sparkConf)
      .build()
      .get()

    assert(spark.read.cassandraFormat("countries", MockCassandra.keyspace).load().count() === 20)
  }

  test("SparkSessionBuilder exception thrown") {
    //    assertThrows[UnknownException.Environment](new SparkSessionBuilder().setEnv("hahaha"))
    assertThrows[IllegalArgumentException](new SparkSessionBuilder("cassandra").setEnv("prod").getOrCreate())
  }


}
