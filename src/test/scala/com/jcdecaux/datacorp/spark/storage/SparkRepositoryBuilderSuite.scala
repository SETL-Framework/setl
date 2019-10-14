package com.jcdecaux.datacorp.spark.storage

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.sql.{Date, Timestamp}

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.config.Properties
import com.jcdecaux.datacorp.spark.enums.{Storage, ValueType}
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.storage.connector.{ExcelConnector, ParquetConnector}
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject, TestObject2}
import com.typesafe.config.Config
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkRepositoryBuilderSuite extends FunSuite with EmbeddedCassandra with BeforeAndAfterAll {
  import SparkRepositorySuite.deleteRecursively
  import SparkTemplate.defaultConf

  override def clearCache(): Unit = CassandraConnector.evictCache()

  var mockCassandra: MockCassandra = _
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockCassandra = new MockCassandra(connector, MockCassandra.keyspace)
      .generateKeyspace()
      .generateTraffic("traffic")
  }

  test("spark repository builder configuration") {

    val builder = new SparkRepositoryBuilder[TestObject]()

    builder.setPath("my/test/path")
    assert(builder.getAs[String]("path").get === "my/test/path")

    builder.setSuffix("adp")
    assert(builder.getAs[String]("path").get === "my/test/path/adp")

  }

  test("SparkRepository cassandra") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

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

  test("SparkRepository CSV") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.CSV)

    repoBuilder.setSpark(spark)
    repoBuilder.setPath("src/test/resources/test_csv")

    val repo = repoBuilder.build().get()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.getAs[String]("path").get))
  }

  test("SparkRepository JSON access") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.JSON)

    repoBuilder.setSpark(spark)
    repoBuilder.setPath("src/test/resources/test_json")
    repoBuilder.setInferSchema(false)
    repoBuilder.setSchema(StructType(Array(
      StructField("partition1", IntegerType),
      StructField("partition2", StringType),
      StructField("clustering1", StringType),
      StructField("value", LongType)
    )))

    val repo = repoBuilder.build().get()
    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.getAs[String]("path").get))
  }

  test("SparkRepository Parquet") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.PARQUET)
      .setSpark(spark)
      .setTable("test")
      .setPath("src/test/resources/test_parquet")

    val repo = repoBuilder.getOrCreate()

    repo.save(testTable, Some("2"))
    repo.save(testTable, Some("x"))
    repo.save(testTable, Some("y"))
    repo.findAll().show()
    assert(repo.findAll().count() === 9)
    repo.getConnector.asInstanceOf[ParquetConnector].delete()

  }

  test("Excel") {


    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

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
      .setInferSchema(false)
      .setTable("test")
      .setSpark(spark)
      .setPath("src/test/resources/test_excel.xlsx")
      .setSchema(schema)

    val repo = repoBuilder.getOrCreate()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.getAs[String]("path").get))

  }

  test("SparkRepository Customized connector") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

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

  test("SparkRepository Build with connector configuration") {
    sparkRepositoryBuilderWithConfigTest(Properties.csvConfigRepoBuilder)
    deleteRecursively(new File(Properties.csvConfigRepoBuilder.getString("path")))

    sparkRepositoryBuilderWithConfigTest(Properties.parquetConfigRepoBuilder)
    deleteRecursively(new File("src/test/resources/test_config_parquet4")) // do not use Properties.parquetConfigRepoBuilder.getPath

    sparkRepositoryBuilderWithConfigTest(Properties.excelConfigRepoBuilder)
    deleteRecursively(new File(Properties.excelConfigRepoBuilder.getString("path")))

    sparkRepositoryBuilderWithConfigTest(Properties.cassandraConfigRepoBuilder)
    //    deleteRecursively(new File(Properties.csvConfig.getString("path")))

  }

  test("SparkRepository throw exceptions") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    // NullPointerException should be thrown if spark session is not set
    assertThrows[NullPointerException](new SparkRepositoryBuilder[TestObject](Storage.OTHER).build())

    // UnknownException.Storage should be thrown if the given storage type is not supported
    assertThrows[UnknownException.Storage](new SparkRepositoryBuilder[TestObject](Storage.OTHER).setSpark(spark).build())

    // NoSuchElementException should be thrown if missing arguments
    assertThrows[InvocationTargetException](new SparkRepositoryBuilder[TestObject](Storage.CSV).setSpark(spark).build())
    //    assertThrows[NoSuchElementException](new SparkRepositoryBuilder[TestObject](Storage.CSV).setSpark(spark).build())
  }

  private[this] def sparkRepositoryBuilderWithConfigTest(config: Config): Unit = {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(defaultConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repo = new SparkRepositoryBuilder[TestObject](config).setSpark(spark).build().get()
    repo.save(testTable)
    val df = repo.findAll()

    df.show(false)
    assert(df.count() === 3)
  }
}
