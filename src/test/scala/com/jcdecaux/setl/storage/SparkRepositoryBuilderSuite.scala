package com.jcdecaux.setl.storage

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.sql.{Date, Timestamp}

import com.datastax.spark.connector.cql.CassandraConnector
import com.jcdecaux.setl.config.Properties
import com.jcdecaux.setl.enums.{Storage, ValueType}
import com.jcdecaux.setl.exception.UnknownException
import com.jcdecaux.setl.storage.connector.{CSVConnector, ExcelConnector, ParquetConnector}
import com.jcdecaux.setl.{MockCassandra, SparkSessionBuilder, TestObject, TestObject2}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.{Await, Future}

class SparkRepositoryBuilderSuite extends AnyFunSuite {

  import SparkRepositorySuite.deleteRecursively

  test("spark repository builder configuration") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val builder = new SparkRepositoryBuilder[TestObject]()

    builder.setPath("my/test/path")
    assert(builder.getAs[String]("path").get === "my/test/path")

    val builder2 = new SparkRepositoryBuilder[TestObject]()
    assertThrows[com.jcdecaux.setl.exception.UnknownException.Storage](builder2.build())

    val builder3 = new SparkRepositoryBuilder[TestObject](spark)
    builder3.setPath("my/test/path")
    assert(builder3.getAs[String]("path").get === "my/test/path")

    val builder4 = new SparkRepositoryBuilder[TestObject](Some(spark), None, None)
    builder4.setPath("my/test/path")
    assert(builder4.getAs[String]("path").get === "my/test/path")
  }

  test("SparkRepository configuration setters") {
    val builder2 = new SparkRepositoryBuilder[TestObject]()

    assert(builder2.getAs[String]("delimiter").get === ";")
    assert(builder2.getAs[Boolean]("inferSchema").get === true)
    assert(builder2.getAs[Boolean]("useHeader").get === true)
    assert(builder2.getAs[Boolean]("header").get === true)
    assert(builder2.getAs[String]("saveMode").get === "Overwrite")
    assert(builder2.getAs[String]("dataAddress").get === "A1")
    assert(builder2.getAs[Boolean]("treatEmptyValuesAsNulls").get === true)
    assert(builder2.getAs[Boolean]("addColorColumns").get === false)
    assert(builder2.getAs[String]("timestampFormat").get === "yyyy-mm-dd hh:mm:ss.000")
    assert(builder2.getAs[String]("dateFormat").get === "yyyy-mm-dd")
    assert(builder2.getAs[Long]("excerptSize").get === 10L)

    builder2.setStorage(Storage.CSV)
    builder2.setClusteringKeys(Some(Seq("clustering1")))
    builder2.setDelimiter("|")
    builder2.setInferSchema(false)
    builder2.setUseHeader(false)
    builder2.setHeader(false)
    builder2.setSaveMode(SaveMode.ErrorIfExists)
    builder2.setDataAddress("B")
    builder2.setTreatEmptyValuesAsNulls(false)
    builder2.setAddColorColumns(true)
    builder2.setTimestampFormat("yyyy-MM-dd hh:mm:ss")
    builder2.setDateFormat("MM-yyyy-dd")
    builder2.setMaxRowsInMemory(2000)
    builder2.setExcerptSize(100L)
    builder2.setWorkbookPassword("password")
    assert(builder2.getAs[String]("storage").get === "CSV")
    assert(builder2.getAs[String]("clusteringKeyColumns") === Some("clustering1"))
    assert(builder2.getAs[String]("delimiter") === Some("|"))
    assert(builder2.getAs[Boolean]("inferSchema").get === false)
    assert(builder2.getAs[Boolean]("useHeader").get === false)
    assert(builder2.getAs[Boolean]("header").get === false)
    assert(builder2.getAs[String]("saveMode").get === "ErrorIfExists")
    assert(builder2.getAs[String]("dataAddress").get === "B")
    assert(builder2.getAs[Boolean]("treatEmptyValuesAsNulls").get === false)
    assert(builder2.getAs[Boolean]("addColorColumns").get === true)
    assert(builder2.getAs[String]("timestampFormat").get === "yyyy-MM-dd hh:mm:ss")
    assert(builder2.getAs[String]("dateFormat").get === "MM-yyyy-dd")
    assert(builder2.getAs[Long]("maxRowsInMemory").get === 2000L)
    assert(builder2.getAs[Long]("excerptSize").get === 100L)
    assert(builder2.getAs[String]("workbookPassword").get === "password")

  }

  test("SparkRepository build with typesafe config") {
    val conf = ConfigFactory.load("test_priority.conf")
    val builder = new SparkRepositoryBuilder[TestObject](conf)
    assertThrows[UnknownException.Storage](builder.build())
  }

  test("SparkRepository cassandra") {
    val connector: CassandraConnector = CassandraConnector(MockCassandra.cassandraConf)

    new MockCassandra(connector, MockCassandra.keyspace)
      .dropKeyspace()
      .generateKeyspace()
      .generateTraffic("traffic")

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
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
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.CSV)

    repoBuilder.setPath("src/test/resources/test_csv")

    val repo = repoBuilder.build().get()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.getAs[String]("path").get))
  }

  test("SparkRepository JSON access") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.JSON)

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
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repoBuilder = new SparkRepositoryBuilder[TestObject](Storage.PARQUET)
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
      .withSparkConf(MockCassandra.cassandraConf)
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

    assertThrows[Throwable](repoBuilder.setSchema(null))

    repoBuilder
      .setInferSchema(false)
      .setTable("test")
      .setPath("src/test/resources/test_excel.xlsx")
      .setSchema(schema)

    val repo = repoBuilder.getOrCreate()

    repo.save(testTable)
    assert(repo.findAll().count() === 3)
    deleteRecursively(new File(repoBuilder.getAs[String]("path").get))

  }

  test("SparkRepository Customized connector") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
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
    // deleteRecursively(new File(Properties.csvConfig.getString("path")))

    sparkRepositoryBuilderWithConfigTest(Properties.deltaConfigRepoBuilder)
    deleteRecursively(new File(Properties.deltaConfigRepoBuilder.getString("path")))
  }

  test("SparkRepository throw exceptions") {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .getOrCreate()

    // UnknownException.Storage should be thrown if the given storage type is not supported
    assertThrows[UnknownException.Storage](new SparkRepositoryBuilder[TestObject](Storage.OTHER).build())

    // NoSuchElementException should be thrown if missing arguments
    assertThrows[InvocationTargetException](new SparkRepositoryBuilder[TestObject](Storage.CSV).build())
    //    assertThrows[NoSuchElementException](new SparkRepositoryBuilder[TestObject](Storage.CSV).setSpark(spark).build())
  }

  private[this] def sparkRepositoryBuilderWithConfigTest(config: Config): Unit = {

    val spark: SparkSession = new SparkSessionBuilder("cassandra")
      .withSparkConf(MockCassandra.cassandraConf)
      .setEnv("local")
      .getOrCreate()

    import spark.implicits._

    val testTable: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val repo = new SparkRepositoryBuilder[TestObject](config).build().get()
    repo.save(testTable)
    val df = repo.findAll()

    df.show(false)
    assert(df.count() === 3)
  }

  test("SparkRepository build StructuredStreaming repo") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    import SparkRepositoryBuilderSuite.StreamingText
    import com.jcdecaux.setl.storage.repository.streaming._
    import spark.implicits._

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._


    val conf = ConfigFactory.load("streaming_test_resources/streaming.conf")

    val inputConf = conf.getConfig("structured_streaming_connector_input_repository")
    val outputConf = conf.getConfig("structured_streaming_connector_output_repository")

    val csvOutputConf: Map[String, String] = Map(
      "path" -> "src/test/resources/streaming_test_resources/output/3",
      "header" -> "true"
    )

    val inputRepo = new SparkRepositoryBuilder[StreamingText](inputConf).getOrCreate()
    val outputRepo = new SparkRepositoryBuilder[StreamingText](outputConf).getOrCreate().toStreamingRepository
    val csvConnector = new CSVConnector(csvOutputConf)

    val input = inputRepo.findAll()

    // Here the future is used only to test awaitTermination() and stop() methods
    // Use should use awaitTerminationOrTimeout() in production code
    val future = Future {
      outputRepo.save(input)
      outputRepo.awaitTermination()
    }

    try {
      Await.result(future, 5 second)
    } catch {
      case _: java.util.concurrent.TimeoutException => outputRepo.stop()
    }

    csvConnector.read().show()
    assert(csvConnector.read().as[String].collect().mkString(" ") === "hello world")
  }

}

object SparkRepositoryBuilderSuite {

  case class StreamingText(text: String) {}

}
