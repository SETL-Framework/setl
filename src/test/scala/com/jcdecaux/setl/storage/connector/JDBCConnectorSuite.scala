package com.jcdecaux.setl.storage.connector

import java.io.ByteArrayOutputStream

import com.jcdecaux.setl.config.{Conf, JDBCConnectorConf, Properties}
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class JDBCConnectorSuite extends AnyFunSuite {

  val input: Seq[(String, String)] = Seq(
    ("a", "A"),
    ("b", "B")
  )

  val url: String = s"jdbc:postgresql://${JDBCConnectorSuite.psqlHost}:5432/framework_dev"
  val user: String = "postgres"
  val password: String = "postgres"

  val options: Map[String, String] = Map(
    "url" -> url,
    "dbtable" -> "unittest",
    "saveMode" -> "Overwrite",
    "user" -> user,
    "password" -> user
  )

  val conf: JDBCConnectorConf = new JDBCConnectorConf()
  conf.set(options)

  test("JDBCConstructor constructors") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val data = input.toDF("col1", "col2")

    val connector1 = new JDBCConnector(
      new JDBCConnectorConf()
        .setUrl(url)
        .setDbTable("test_constructor_1")
        .setUser(user)
        .setPassword(password)
        .setSaveMode(SaveMode.Overwrite)
    )

    val connector2 = new JDBCConnector(url, "test_constructor_2", user, password, SaveMode.Overwrite)

    connector1.write(data)
    connector2.write(data)

    assert(connector1.read().collect().length === 2)
    assert(connector2.read().collect().length === 2)

    connector1.write(data)
    assert(connector1.read().collect().length === 2)
    // assertThrows[org.apache.spark.sql.AnalysisException](connector2.write(data))

    val connector3 = new JDBCConnector(options)
    connector3.write(data)
    assert(connector3.read().collect().length === 2)

    val connector4 = new JDBCConnector(Properties.jdbcConfig)
    connector4.write(data)
    val connector5 = new JDBCConnector(spark, Properties.jdbcConfig)
    assert(connector4.read().collect().length === 2)
    assert(connector5.read().collect().length === 2)

    val connector6 = new JDBCConnector(conf)
    val connector7 = new JDBCConnector(spark, conf)
    assert(connector6.read().collect().length === 2)
    assert(connector7.read().collect().length === 2)
  }

  test("JDBCConnector should read and write data") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val data = input.toDF("col1", "col2")

    val connector = new JDBCConnector(options)
    connector.write(data)
    assert(connector.read().collect().length === 2)

    val connector2 = new JDBCConnector(url, "test_constructor_2", user, password, SaveMode.Overwrite)
    connector2.write(data, Some("options"))
    assert(connector2.read().collect().length === 2)

    val connector3 = new JDBCConnector(
      new JDBCConnectorConf()
        .setUrl(url)
        .setDbTable("test_constructor_2")
        .setUser(user)
        .setPassword(password)
        .setSaveMode(SaveMode.Append)
    )
    connector3.write(data)
    assert(connector3.read().collect().length === 4)
  }

  test("JDBCConnector's create method is not yet implemented") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val logger = Logger.getLogger(classOf[JDBCConnector])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)
    val warnMessage = "Create is not supported in JDBC Connector"
    val data = input.toDF("col1", "col2")

    val connector = new JDBCConnector(options)
    connector.create(data)
    assert(outContent.toString.contains(warnMessage))

    outContent.reset()
    connector.create(data, Some("options"))
    assert(outContent.toString.contains(warnMessage))
  }

  test("JDBCConnector's delete method is not yet implemented") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    val logger = Logger.getLogger(classOf[JDBCConnector])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)

    val connector = new JDBCConnector(options)
    connector.delete("query")
    assert(outContent.toString.contains("Delete is not supported in JDBC Connector"))
  }

}

object JDBCConnectorSuite {
  val psqlHost: String = "localhost"  // System.getProperty("setl.test.postgres.host", "localhost")
}
