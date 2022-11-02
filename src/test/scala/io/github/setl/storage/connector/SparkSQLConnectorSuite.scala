package io.github.setl.storage.connector

import io.github.setl.config.{Conf, Properties}
import io.github.setl.{SparkSessionBuilder, TestObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkSQLConnectorSuite extends AnyFunSuite{

  val query : String =
    """
      | SELECT (ones.n1 + tens.n2 * 10) as user_id
      | FROM (
      |  SELECT 0 AS n1
      |  UNION SELECT 1 AS n1
      |  UNION SELECT 2 AS n1
      |  UNION SELECT 3 AS n1
      |  UNION SELECT 4 AS n1
      |  UNION SELECT 5 AS n1
      |  UNION SELECT 6 AS n1
      |  UNION SELECT 7 AS n1
      |  UNION SELECT 8 AS n1
      |  UNION SELECT 9 AS n1
      | ) ones
      | CROSS JOIN
      | (
      |  SELECT 0 AS n2
      |  UNION SELECT 1 AS n2
      |  UNION SELECT 2 AS n2
      |  UNION SELECT 3 AS n2
      |  UNION SELECT 4 AS n2
      |  UNION SELECT 5 AS n2
      |  UNION SELECT 6 AS n2
      |  UNION SELECT 7 AS n2
      |  UNION SELECT 8 AS n2
      |  UNION SELECT 9 AS n2
      | ) tens
      |""".stripMargin

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  val options : Map[String, String] = Map(
    "query" -> query
  )


  test("Instantiation of constructors") {
    val connector = new SparkSQLConnector(query)
    assert(connector.query === query)

    val testConfig = Properties.sparkSQLConfig
    val connector2 = new SparkSQLConnector(testConfig)
    assert(connector2.query === "SELECT * FROM schema.table")

    val connector3 = new SparkSQLConnector(Conf.fromMap(options))
    assert(connector3.query === query)

    assertThrows[IllegalArgumentException](new SparkSQLConnector(""))
    assertThrows[IllegalArgumentException](new SparkSQLConnector(Conf.fromMap(Map.empty)))
    assertThrows[IllegalArgumentException](new SparkSQLConnector(testConfig.withoutPath("query")))
  }

  test("Read/Write of SparkSQLConnector") {
    val spark: SparkSession = SparkSession.builder().config(new SparkConf()).master("local[*]").getOrCreate()
    import spark.implicits._

    val connector = new SparkSQLConnector(query)
    assert(connector.read().collect().length == 100)

    // Should log warning & do nothing
    val testDF = testTable.toDF()
    connector.write(testDF)
    connector.write(testDF, Some("any_"))
  }
}
