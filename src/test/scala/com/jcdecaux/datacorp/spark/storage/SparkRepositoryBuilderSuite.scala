package com.jcdecaux.datacorp.spark.storage

import java.io.File
import java.sql.{Date, Timestamp}

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.enums.{Storage, ValueType}
import com.jcdecaux.datacorp.spark.storage.v2.connector.ExcelConnector
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject, TestObject2}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkRepositoryBuilderSuite extends FunSuite with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {

  import SparkRepositorySuite.deleteRecursively

  override def clearCache(): Unit = CassandraConnector.evictCache()

  val spark: SparkSession = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()

  import spark.implicits._

  var mockCassandra: MockCassandra = _
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockCassandra = new MockCassandra(connector, MockCassandra.keyspace)
      .generateKeyspace()
      .generateTraffic("traffic")
  }

  test("cassandra") {

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repo = new SparkRepositoryBuilder[TestObject](Storage.CASSANDRA)
      .setKeyspace(MockCassandra.keyspace)
      .setTable("test")
      .setSpark(spark)
      .setPartitionKeys(Some(Seq("partition1")))
      .build().get()

    val conditions = Set(
      Condition("partition1", "=", Some("1"), ValueType.NUMBER)
    )

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    assert(repo.findBy(conditions).count() === 1)
    assert(repo.findBy(Condition("partition1", "=", Some("1"), ValueType.NUMBER)).count() === 1)
    assert(repo.findBy(conditions).head() === repo.findBy(Condition("partition1", "=", Some("1"), ValueType.NUMBER)).head())
  }

  test("CSV") {

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.CSV)

    repoBuilder.spark = spark
    repoBuilder.path = "src/test/resources/test_csv"

    val repo = repoBuilder.build().get()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.path))

  }

  test("Parquet") {
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.PARQUET)

    repoBuilder.table = "test"
    repoBuilder.spark = spark
    repoBuilder.path = "src/test/resources/test_parquet"

    val repo = repoBuilder.build().get()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.path))

  }

  test("Excel") {
    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val schema: StructType = StructType(Array(
      StructField("partition1", IntegerType),
      StructField("partition2", StringType),
      StructField("clustering1", StringType),
      StructField("value", LongType)
    ))

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.EXCEL)

    repoBuilder
      .inferSchema(false)
      .setTable("test")
      .setSpark(spark)
      .setPath("src/test/resources/test_excel.xlsx")
      .setSchema(schema)

    val repo = repoBuilder.build().get()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.path))

  }

  test("Customized connector") {
    val path: String = "src/test/resources/test_excel.xlsx"

    val testTable: Dataset[TestObject2] = Seq(
      TestObject2("string", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
      TestObject2("string2", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
      TestObject2("string3", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L)
    ).toDS()

    val schema: StructType = StructType(Array(
      StructField("col1", StringType),
      StructField("col2", IntegerType),
      StructField("col3", DoubleType),
      StructField("col4", TimestampType),
      StructField("col5", DateType),
      StructField("col6", LongType)
    ))

    val excelConnector = new ExcelConnector(
      spark,
      path,
      useHeader = "true",
      timestampFormat = "dd/mm/yyyy hh:mm:ss",
      dateFormat = "dd/mm/yy",
      schema = Some(schema)
    )

    val repoBuilder = new SparkRepositoryBuilder[TestObject2](Storage.EXCEL)
    val repo = repoBuilder.setConnector(excelConnector).build().get()

    repo.save(testTable)

    val df = repo.findAll()

    df.show(false)
    df.printSchema()
    assert(df.head.col4 === new Timestamp(1557153268000L))
    assert(df.head.col5.getTime === 1557100800000L)

    deleteRecursively(new File(path))
  }


}
