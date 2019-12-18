package com.jcdecaux.setl.storage.connector

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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
  }

}

object DynamoDBConnectorSuite {

  val host: String = System.getProperty("aws.dynamodb.endpoint", "http://localhost:8000")

}
