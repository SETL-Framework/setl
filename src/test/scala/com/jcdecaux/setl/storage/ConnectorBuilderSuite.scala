package com.jcdecaux.setl.storage

import java.io.File

import com.datastax.spark.connector.cql.{CassandraConnector => CC}
import com.jcdecaux.setl.config.{Conf, Properties}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.exception.{ConfException, UnknownException}
import com.jcdecaux.setl.storage.SparkRepositorySuite.deleteRecursively
import com.jcdecaux.setl.storage.connector.{CSVConnector, DeltaConnector, JSONConnector, StructuredStreamingConnector, StructuredStreamingConnectorSuite}
import com.jcdecaux.setl.{MockCassandra, SparkSessionBuilder, SparkTestUtils, TestObject}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ConnectorBuilderSuite extends AnyFunSuite with BeforeAndAfterAll {

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  test("Deprecated constructors") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val conf = new Conf()
    conf.set("storage", "CSV")
    conf.set("path", "src/test/resources/test_config_csv3")
    conf.set("inferSchema", "true")
    conf.set("header", "true")

    val connector = new ConnectorBuilder(Properties.csvConfigConnectorBuilder).build().get()
    connector.write(testTable.toDF())

    val connector2 = new ConnectorBuilder(spark, Some(Properties.csvConfigConnectorBuilder), None).build().get()
    val connector3 = new ConnectorBuilder(spark, Properties.csvConfigConnectorBuilder).build().get()
    val connector4 = new ConnectorBuilder(spark, conf).build().get()

    assert(connector.read().count() === 3)
    assert(connector2.read().count() === 3)
    assert(connector3.read().count() === 3)
    assert(connector4.read().count() === 3)

    deleteRecursively(new File(Properties.csvConfigConnectorBuilder.getString("path")))
  }

  test("build cassandra connector") {
    val _connector: CC = CC(MockCassandra.cassandraConf)
    new MockCassandra(_connector, "test_space")
      .generateKeyspace()
      .generateCountry("countries")

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .build()
      .get()

    assertThrows[IllegalArgumentException](
      new ConnectorBuilder(Some(Properties.wrongCsvConfigConnectorBuilder2), Some(new Conf().set("storage", "BLABLA"))).build().get()
    )
  }

  test("build structured streaming connector") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    import spark.implicits._

    val conf = ConfigFactory.load("streaming_test_resources/streaming.conf")

    val inputConf = conf.getConfig("structured_streaming_connector_input")
    val outputConf = conf.getConfig("structured_streaming_connector_output")

    val csvOutputConf: Map[String, String] = Map(
      "path" -> "src/test/resources/streaming_test_resources/output/2",
      "header" -> "false"
    )

    val connector = new ConnectorBuilder(inputConf).getOrCreate()
    val outputConnector = new ConnectorBuilder(outputConf).getOrCreate()
    val parquetConnector = new CSVConnector(csvOutputConf)

    val input = connector.read()

    outputConnector.write(input)
    outputConnector.asInstanceOf[StructuredStreamingConnector].awaitTerminationOrTimeout(10000)

    parquetConnector.read().show()
    assert(parquetConnector.read().as[String].collect().mkString(" ") === StructuredStreamingConnectorSuite.text)

  }

  test("build DeltaConnector") {
    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .build()
      .get()
    assume(SparkTestUtils.checkSparkVersion("2.4.2"))

    import spark.implicits._
    val connector = new ConnectorBuilder(Properties.deltaConfigConnectorBuilder).build().get()

    testTable.toDF.show()
    connector.write(testTable.toDF)

    val df = connector.read()

    df.show()
    assert(df.count() === 3)
    connector.asInstanceOf[DeltaConnector].drop()
  }
}
