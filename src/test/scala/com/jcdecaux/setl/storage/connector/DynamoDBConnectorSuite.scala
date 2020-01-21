package com.jcdecaux.setl.storage.connector

import java.io.ByteArrayOutputStream

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.jcdecaux.setl.config.{Conf, DynamoDBConnectorConf}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Logger, SimpleLayout, WriterAppender}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DynamoDBConnectorSuite extends AnyFunSuite {

  // SHOULD BE MORE THAN 16 ROWS OTHERWISE SPARK-DYNAMODB CONNECTOR CAN'T INFER SCHEMA
  val input: Seq[(String, String)] = Seq(
    ("a", "A"),
    ("b", "B"),
    ("c", "C"),
    ("d", "D"),
    ("aa", "A"),
    ("bb", "B"),
    ("cc", "C"),
    ("dd", "D"),
    ("aaa", "A"),
    ("bbb", "B"),
    ("ccc", "C"),
    ("ddd", "D"),
    ("aaaa", "A"),
    ("bbbb", "B"),
    ("cccc", "C"),
    ("dddd", "D"),
    ("aaaaa", "A"),
    ("bbbbb", "B"),
    ("ccccc", "C"),
    ("ddddd", "D")
  )

  val conf: Map[String, String] = Map(
    "region" -> "eu-west-1",
    "table" -> "test-table",
    "throughput" -> "10000",
    "update" -> "true",
    "defaultParallelism" -> "100"
  )

  val credential = new BasicAWSCredentials("fakeAccess", "fakeSecret")
  val endpoint = new EndpointConfiguration(DynamoDBConnectorSuite.host, "eu-west-1")

  println("client")
  val client: AmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(credential))
    .withEndpointConfiguration(endpoint)
    .build()

  println("request")
  val createTableRequest: CreateTableRequest = new CreateTableRequest()
    .withAttributeDefinitions(
      new AttributeDefinition("col1", ScalarAttributeType.S),
      new AttributeDefinition("col2", ScalarAttributeType.S)
    )
    .withKeySchema(
      new KeySchemaElement("col1", KeyType.HASH),
      new KeySchemaElement("col2", KeyType.RANGE)
    )
    .withProvisionedThroughput(new ProvisionedThroughput(10000L, 10000L))
    .withTableName("test-table")

  println("send request")

  println(client.listTables())

  try {
    client.deleteTable("test-table")
  } catch {
    case _: Throwable =>
  }

  client.createTable(createTableRequest)

  test("DynamoDBConnector should read and write") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._

    val data = input.toDF("col1", "col2")
    val connector = new DynamoDBConnector(conf)

    data.show()
    connector.write(data)

    val readData = connector.read()
    assert(readData.collect().length === input.length)

    val connector2 = new DynamoDBConnector("eu-west-1", "test-table", SaveMode.ErrorIfExists, "10000")
    val connector3 = new DynamoDBConnector(spark, "eu-west-1", "test-table", SaveMode.ErrorIfExists, "10000")
    assert(connector2.read().collect().length === input.length)
    assert(connector3.read().collect().length === input.length)


    val dynamoDBConnectorConf = new DynamoDBConnectorConf()
      .setTable("test-table")
      .set(DynamoDBConnectorConf.REGION, "eu-west-1")
      .set(DynamoDBConnectorConf.THROUGHPUT, "1000")

    val connector4 = new DynamoDBConnector(dynamoDBConnectorConf)
    val connector4b = new DynamoDBConnector(spark, dynamoDBConnectorConf)
    assert(connector4.read().collect().length === input.length)
    assert(connector4b.read().collect().length === input.length)

    val dynamoConf = new Conf()
      .set(DynamoDBConnectorConf.TABLE, "test-table")
      .set(DynamoDBConnectorConf.REGION, "eu-west-1")
      .set(DynamoDBConnectorConf.THROUGHPUT, "1000")
    val connector5 = new DynamoDBConnector(dynamoConf)
    val connector5b = new DynamoDBConnector(spark, dynamoConf)
    assert(connector5.read().collect().length === input.length)
    assert(connector5b.read().collect().length === input.length)

    val config = ConfigFactory.load("dynamodb.conf")
    assertThrows[IllegalArgumentException](new DynamoDBConnector(config))


    val connector7 = new DynamoDBConnector(config.getConfig("dynamodb.connector"))
    val connector7b = new DynamoDBConnector(spark, config.getConfig("dynamodb.connector"))
    assert(connector7.read().collect().length === input.length)
    assert(connector7b.read().collect().length === input.length)

  }

  test("DynamoDB Connector un-implemented methods") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val logger = Logger.getLogger(classOf[DynamoDBConnector])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)

    val connector = new DynamoDBConnector(conf)
    val data = input.toDF("col1", "col2")
    connector.delete("")
    assert(outContent.toString.contains("Delete is not supported in DynamoDBConnector"))

    outContent.reset()
    connector.create(data)
    assert(outContent.toString.contains("Create is not supported in DynamoDBConnector"))

    outContent.reset()
    connector.create(data, Some("suffix"))
    assert(outContent.toString.contains("Create is not supported in DynamoDBConnector"))

    outContent.reset()
    connector.write(data, Some("suffix"))
    assert(outContent.toString.contains("Suffix will be ignored in DynamoDBConnector"))
  }

}

object DynamoDBConnectorSuite {

  val host: String = "http://localhost:8000"  // System.getProperty("aws.dynamodb.endpoint", "http://localhost:8000")

}
