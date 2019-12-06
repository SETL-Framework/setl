package com.jcdecaux.datacorp.spark.storage

import java.io.File

import com.datastax.spark.connector.cql.{CassandraConnector => CC}
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.config.{Conf, Properties}
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.{ConfException, UnknownException}
import com.jcdecaux.datacorp.spark.storage.SparkRepositorySuite.deleteRecursively
import com.jcdecaux.datacorp.spark.storage.connector.JSONConnector
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ConnectorBuilderSuite extends AnyFunSuite with EmbeddedCassandra with BeforeAndAfterAll {

  import SparkTemplate.defaultConf

  override def clearCache(): Unit = CC.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  val _connector: CC = CC(defaultConf)

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(_connector, "test_space")
      .generateKeyspace()
      .generateCountry("countries")
  }

  test("build cassandra connector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    import spark.implicits._

    val connector = new ConnectorBuilder(Properties.cassandraConfigConnectorBuilder).build().get()

    // Create table and write data
    connector.write(testTable.toDF())

    // read table
    val readTable = connector.read()
    readTable.show()
    assert(readTable.count() === 3)
  }

  test("build csv connector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    import spark.implicits._

    val connector = new ConnectorBuilder(Properties.csvConfigConnectorBuilder).build().get()

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    deleteRecursively(new File(Properties.csvConfigConnectorBuilder.getString("path")))
  }

  test("build parquet connector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    import spark.implicits._
    val connector = new ConnectorBuilder(Properties.parquetConfigConnectorBuilder).build().get()

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    deleteRecursively(new File("src/test/resources/test_config_parquet3")) // do not use Properties.parquetConfigRepoBuilder.getPath
  }

  test("build excel connector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    import spark.implicits._
    val connector = new ConnectorBuilder(Properties.excelConfigConnectorBuilder).build().get()

    testTable.toDF.show()
    connector.write(testTable.toDF)

    val df = connector.read()

    df.show()
    assert(df.count() === 3)
    deleteRecursively(new File(Properties.excelConfigConnectorBuilder.getString("path")))
  }

  test("build JSONConnector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    import spark.implicits._
    val connector = new ConnectorBuilder(Properties.jsonConfigConnectorBuilder).build().get()

    testTable.toDF.show()
    connector.write(testTable.toDF)

    val df = connector.read()

    df.show()
    assert(df.count() === 3)
    connector.asInstanceOf[JSONConnector].delete()
  }

  test("wrong builder configuration") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()


    // IllegalArgumentException should be thrown when the typesafe config contains a wrong storage type
    assertThrows[IllegalArgumentException](new ConnectorBuilder(Properties.wrongCsvConfigConnectorBuilder).build().get())

    // ConfException should be thrown when the storage type can't be parsed
    assertThrows[ConfException](
      new ConnectorBuilder(new Conf().set("storage", "BLABLA")).build().get()
    )

    // UnknownException.Storage should be thrown if the given storage is not supported
    assertThrows[UnknownException.Storage](new ConnectorBuilder(Properties.wrongCsvConfigConnectorBuilder2).build().get())
    assertThrows[UnknownException.Storage](new ConnectorBuilder(new Conf().set("storage", Storage.OTHER)).build().get())
  }

  test("Connector builder with two configurations") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .build()
      .get()

    assertThrows[IllegalArgumentException](
      new ConnectorBuilder(Some(Properties.wrongCsvConfigConnectorBuilder2), Some(new Conf().set("storage", "BLABLA"))).build().get()
    )
  }

}
