package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.config.JDBCConnectorConf
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

  val conf: Map[String, String] = Map(
    "url" -> url,
    "dbtable" -> "unittest",
    "saveMode" -> "Overwrite",
    "user" -> user,
    "password" -> user
  )

  test("JDBCConstructor should work") {
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

  }

  test("JDBCConnector should read and write data") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._
    val data = input.toDF("col1", "col2")

    val connector = new JDBCConnector(conf)
    connector.write(data)
    assert(connector.read().collect().length === 2)
  }

}

object JDBCConnectorSuite {
  val psqlHost: String = "localhost"  // System.getProperty("setl.test.postgres.host", "localhost")
}
