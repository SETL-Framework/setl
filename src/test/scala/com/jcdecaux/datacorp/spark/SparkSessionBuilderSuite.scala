package com.jcdecaux.datacorp.spark

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.enums.AppEnv
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.scalatest.{BeforeAndAfterAll, FunSuite, SequentialNestedSuiteExecution}


class SparkSessionBuilderSuite extends FunSuite with BeforeAndAfterAll with SequentialNestedSuiteExecution with EmbeddedCassandra with SparkTemplate {

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()

    new MockCassandra(connector, MockCassandra.keyspace)
      .generateKeyspace()
      .generateCountry("countries")
  }

  test("Default instantiation") {
    val sparkSessionBuilder = new SparkSessionBuilder()

    assert(sparkSessionBuilder.appEnv === AppEnv.DEV)
    assert(sparkSessionBuilder.appName === "SparkApplication")
    assert(sparkSessionBuilder.cassandraHost === null)
    assert(sparkSessionBuilder.config.getClass === classOf[SparkConf])
    assert(sparkSessionBuilder.initialization === true)
    assert(sparkSessionBuilder.spark === null)

    sparkSessionBuilder.build()

    assert(sparkSessionBuilder.spark != null)
  }

  test("Cassandra connection") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .setEnv("dev")
      .setCassandraHost("localhost")
      .build()
      .get()

    assert(spark.read.cassandraFormat("countries", MockCassandra.keyspace).load().count() === 20)
  }

  test("Custom configuration") {

    val sparkConf = new SparkConf()
      .setAppName("CustomConfigApp")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")

    //    println(sparkConf.get("spark.cassandra.connection.port"))

    val spark = new SparkSessionBuilder()
      .configure(sparkConf)
      .build()
      .get()

    assert(spark.read.cassandraFormat("countries", MockCassandra.keyspace).load().count() === 20)
  }


}
