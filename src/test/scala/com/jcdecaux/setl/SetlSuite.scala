package com.jcdecaux.setl

import java.util.concurrent.atomic.AtomicReference

import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.config.ConfigLoader
import com.jcdecaux.setl.storage.connector.{CSVConnector, Connector, FileConnector}
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.storage.{Condition, ConnectorBuilder, SparkRepositoryBuilder}
import com.jcdecaux.setl.transformation.Factory
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class SetlSuite extends AnyFunSuite with PrivateMethodTester {

  val configLoader: ConfigLoader = ConfigLoader.builder()
    .setAppEnv("local")
    .setAppName("Test ConfigLoader builder")
    .getOrCreate()

  val ds: Seq[TestObject] = Seq(
    TestObject(1, "a", "A", 1L),
    TestObject(2, "b", "B", 2L)
  )

  test("Setl should build a spark session") {
    val context: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .setSetlConfigPath("context")
      .setShufflePartitions(500)
      .getOrCreate()

    val ss = context.spark

    println(configLoader.appName)
    println(ss.sparkContext.appName)
    println(ss.sparkContext.getConf.get("spark.app.name"))

    assert(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "500",
      "setShufflePartition should overwrite config file")
    assert(ss.sparkContext.appName === configLoader.appName)
  }

  test("Setl should handle parameter setting") {
    val context2: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .setSetlConfigPath("context")
      .getOrCreate()
    assert(context2.spark.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "600")

    val context: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .setSetlConfigPath("context")
      .setParallelism(500)
      .getOrCreate()

    assert(context.spark.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "500",
      "setShufflePartition should overwrite config file")
  }

  test("Setl should handle setl configuration path") {
    val context: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .setSetlConfigPath("setl.config")
      .getOrCreate()

    val ss = context.spark

    println(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
    println(ss.sparkContext.getConf.get("spark.app.name"))

    assert(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "1000")
  }

  test("Setl should handle spark configuration in non local environment (with default config path)") {
    val context: Setl = Setl.builder()
      .withDefaultConfigLoader("test_priority")
      .getOrCreate()

    val ss = context.spark

    println(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
    println(ss.sparkContext.getConf.get("spark.app.name"))

    assert(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "1000")
    assert(ss.sparkContext.getConf.get("spark.app.name") === "my_app_2")
  }

  test("Setl should handle spark configuration in non local environment (with specific config path)") {
    val context: Setl = Setl.builder()
      .withDefaultConfigLoader("test_priority")
      .setSetlConfigPath("setl.config_2")
      .getOrCreate()

    val ss = context.spark

    println(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
    println(ss.sparkContext.getConf.get("spark.app.name"))

    assert(ss.sparkContext.getConf.get("spark.sql.shuffle.partitions") === "2000")
    assert(ss.sparkContext.getConf.get("spark.app.name") === "my_app_context_2")
  }

  test("Setl should be able to create SparkRepository") {
    val context: Setl = Setl.builder()
      .setConfigLoader(configLoader)
      .setSetlConfigPath("context")
      .getOrCreate()

    val ss = context.spark

    import ss.implicits._

    val ds = this.ds.toDS()

    val repo = context.getSparkRepository[TestObject]("csv_dc_context2")

    repo.save(ds)
    val read = repo.findAll()

    assert(read.count() === 2)
    assert(repo.findBy(Condition("partition1", "=", 1)).count() === 1)

    repo.getConnector.asInstanceOf[FileConnector].delete()
  }

  test("Setl should be able to create a pipeline with all the registered spark repository") {
    val context = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    val repo1 = context.getSparkRepository[TestObject]("csv_dc_context")
    val repo2 = context.getSparkRepository[TestObject3]("parquet_dc_context")

    context
      .newPipeline()
      .addStage(classOf[SetlSuite.MyFactory], context.spark)
      .addStage(classOf[SetlSuite.MyFactory2], context.spark)
      .run()
      .getLastOutput.asInstanceOf[Dataset[TestObject3]]
      .show()

    assert(repo1.findAll().count() === 2)
    assert(repo2.findAll().count() === 2)
    assert(repo1.findAll().columns.length === 4)
    assert(repo2.findAll().columns.length === 5)

    repo1.getConnector.asInstanceOf[FileConnector].delete()
    repo2.getConnector.asInstanceOf[FileConnector].delete()

    assert(context.configLoader.configPath === None)
    assert(context.configLoader.appEnv === "local")
  }

  test("Setl should be work with default config loader") {

    val context = Setl.builder()
      .withDefaultConfigLoader("myconf.conf")
      .getOrCreate()

    assert(context.configLoader.get("my_test_variable") === "haha")
    assert(context.configLoader.appEnv === "local")
    assert(context.configLoader.configPath === Some("myconf.conf"))
    System.clearProperty("app.environment")
  }

  test("SparkException should be thrown when no master url is configured") {
    System.clearProperty("app.environment")
    System.setProperty("app.environment", "dev")
    val context = Setl.builder().withDefaultConfigLoader("myconf.conf")

    assertThrows[SparkException](context.getOrCreate())
    System.clearProperty("app.environment")
  }

  test("User should be able to set master url") {
    System.clearProperty("app.environment")
    System.setProperty("app.environment", "dev")
    val context = Setl.builder()
      .withDefaultConfigLoader("myconf.conf")
      .setSparkMaster("local")
      .getOrCreate()

    assert(context.configLoader.appEnv === "dev")
    System.clearProperty("app.environment")
  }

  test("User should be able to set config file") {
    val context = Setl.builder().withDefaultConfigLoader("test_priority.conf").getOrCreate()
    assert(context.configLoader.get("my.value") === "haha")
  }

  test("Default context config path (app.context) should be used") {
    System.clearProperty("app.environment")
    System.setProperty("app.environment", "dev")
    val context = Setl.builder().withDefaultConfigLoader("test_priority.conf").getOrCreate()
    assert(context.configLoader.get("my.value") === "haha")
    assert(context.spark.conf.get("spark.app.name") === "my_app_2")
    System.clearProperty("app.environment")
  }

  test("Context builder should handle config file path with no extension") {
    System.clearProperty("app.environment")
    System.setProperty("app.environment", "dev")
    val context = Setl.builder().withDefaultConfigLoader("test_priority").getOrCreate()
    assert(context.configLoader.get("my.value") === "haha")
    assert(context.spark.conf.get("spark.app.name") === "my_app_2")
    System.clearProperty("app.environment")
  }

  test("Setl set repository should be able to handle different consumer") {
    val context = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    context.setSparkRepository[TestObject]("csv_dc_context_consumer", Array(classOf[SetlSuite.MyFactory]))
    context.setSparkRepository[TestObject]("parquet_dc_context_consumer")

    context
      .newPipeline()
      .addStage(classOf[SetlSuite.MyFactory], context.spark)
      .run()

    val repo = context.getSparkRepository[TestObject]("csv_dc_context_consumer")
    val conn = repo.getConnector.asInstanceOf[FileConnector]

    assert(conn.basePath.toString === "src/test/resources/test_config_csv_dc_context_consumer")
    conn.delete()
  }

  test("Setl should handle connectors with delivery id") {

    val context = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    context
      .setSparkRepository[TestObject]("csv_dc_context_consumer", Array(classOf[SetlSuite.MyFactory]), readCache = true)
      .setConnector("csv_dc_context_consumer", "1")

    val factory = new SetlSuite.FactoryWithConnectorDeliveryVariable

    context
      .newPipeline()
      .addStage(classOf[SetlSuite.MyFactory], context.spark)
      .addStage(factory)
      .describe()
      .run()

    val output = factory.get()
    output.show()
    assert(output.count() === 2)
    assert(output.collect() === Array(
      TestObject(1, "a", "A", 1L),
      TestObject(2, "b", "B", 2L)
    ))
    context.getConnector("csv_dc_context_consumer").asInstanceOf[FileConnector].delete()
  }

  test("Setl should handle delivery annotated method with deliveryID") {

    val context = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    context
      .setSparkRepository[TestObject]("csv_dc_context_consumer", Array(classOf[SetlSuite.MyFactory]), readCache = true)
      .setConnector("csv_dc_context_consumer", "1", classOf[CSVConnector])

    val factory = new com.jcdecaux.setl.SetlSuite.FactoryWithConnectorDeliveryMethod

    context
      .newPipeline()
      .addStage(classOf[SetlSuite.MyFactory], context.spark)
      .addStage(factory)
      .describe()
      .run()

    val output = factory.get()
    output.show()
    assert(output.count() === 2)
    assert(output.collect() === Array(
      TestObject(1, "a", "A", 1L),
      TestObject(2, "b", "B", 2L)
    ))
    context.getConnector[FileConnector]("csv_dc_context_consumer").delete()
  }

  test("Setl should handle user instantiated repositories and connectors") {

    val context = Setl.builder()
      .setConfigLoader(configLoader)
      .getOrCreate()

    val repo = new SparkRepositoryBuilder[TestObject](context.configLoader.getConfig("csv_dc_context_consumer")).getOrCreate()
    val connector = new ConnectorBuilder(context.configLoader.getConfig("csv_dc_context_consumer")).getOrCreate().asInstanceOf[CSVConnector]

    context
      .setSparkRepository[TestObject](repo, Seq(classOf[SetlSuite.MyFactory]), "", "repository1")
      .setConnector(connector, "1", "connector1")

    val factory = new com.jcdecaux.setl.SetlSuite.FactoryWithConnectorDeliveryMethod

    val pipeline = context
      .newPipeline()
      .addStage(classOf[SetlSuite.MyFactory], context.spark)
      .addStage(factory)
      .describe()
      .run()

    val output = factory.get()
    output.show()
    assert(output.count() === 2)
    assert(output.collect() === Array(
      TestObject(1, "a", "A", 1L),
      TestObject(2, "b", "B", 2L)
    ))
    context.getConnector[FileConnector]("csv_dc_context_consumer").delete()

    assert(context.getPipeline(pipeline.getUUID).nonEmpty)
    assert(context.getPipeline(pipeline.getUUID).get.stages.length === 2)
  }

  test("Setl should take into account use defined SparkSession") {
    try {
      // Stop the active spark context (if exists)
      SparkContext.getOrCreate().stop()
    } catch {
      case _: Throwable =>
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("setl_test_app").set("myKey", "myValue")
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    assertThrows[NoSuchElementException](Setl.builder().setSparkSession(spark).getOrCreate())

    val setl = Setl.builder()
      .setSparkSession(spark)
      .withDefaultConfigLoader()
      .getOrCreate()

    assert(setl.spark.sparkContext.getConf.get("myKey") === "myValue")
    assert(setl.sparkSession.sparkContext.getConf.get("myKey") === "myValue")
    assertThrows[NoSuchElementException](setl.spark.sparkContext.getConf.get("noSuchKey"))
    assert(setl.configLoader.appEnv === "local")
  }

  test("Setl setSparkConf") {
    val sparkConf = new SparkConf().setMaster("local").setAppName("setl_test_app").set("myKey", "myValue")
    val setl = Setl.builder()
      .setSparkConf(sparkConf)
      .withDefaultConfigLoader()
      .getOrCreate()

    assert(setl.sparkSession.sparkContext.getConf.get("myKey") === "myValue")
    assertThrows[NoSuchElementException](setl.spark.sparkContext.getConf.get("noSuchKey"))
    assert(setl.configLoader.appEnv === "local")
  }

  test("Setl should be able to stop the spark session") {

    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
    try {
      // Stop the active spark context (if exists)
      SparkContext.getOrCreate().stop()
    } catch {
      case _: Throwable =>
    }

    assert(SparkSession.getActiveSession.isEmpty)
    assert(SparkSession.getDefaultSession.isEmpty)

    val setl = Setl.builder().withDefaultConfigLoader().getOrCreate()
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getDefaultSession.isDefined)

    setl.stop()
    assert(SparkSession.getActiveSession.isEmpty)
    assert(SparkSession.getDefaultSession.isEmpty)

    // As the SparkContext is stopped, SparkContext.getOrCreate should throw SparkException because
    // no master URL is configured
    assertThrows[SparkException](SparkContext.getOrCreate())
  }

}

object SetlSuite {

  class MyFactory(spark: SparkSession) extends Factory[Dataset[TestObject]] {

    import spark.implicits._

    @Delivery
    private[this] val repo = SparkRepository[TestObject]
    var output: Dataset[TestObject] = Seq(
      TestObject(1, "a", "A", 1L),
      TestObject(2, "b", "B", 2L)
    ).toDS()

    override def read(): MyFactory.this.type = this

    override def process(): MyFactory.this.type = this

    override def write(): MyFactory.this.type = {
      repo.save(output)
      this
    }

    override def get(): Dataset[TestObject] = output
  }

  class MyFactory2(spark: SparkSession) extends Factory[Dataset[TestObject3]] {
    @Delivery
    var input: Dataset[TestObject] = _
    @Delivery
    val repo: SparkRepository[TestObject3] = SparkRepository[TestObject3]
    var output: Dataset[TestObject3] = _

    override def read(): MyFactory2.this.type = this

    override def process(): MyFactory2.this.type = {
      output = input
        .withColumn("value2", functions.lit("haha"))
        .as[TestObject3](ExpressionEncoder[TestObject3])
      this
    }

    override def write(): MyFactory2.this.type = {
      repo.save(output)
      this
    }

    override def get(): Dataset[TestObject3] = output
  }

  class FactoryWithConnectorDeliveryVariable extends Factory[Dataset[TestObject]] {

    @Delivery(id = "1")
    var testObjectConnector: Connector = _

    @Delivery(id = "2", optional = true)
    var anotherConnector: Connector = _

    var output: DataFrame = _

    override def read(): FactoryWithConnectorDeliveryVariable.this.type = {
      output = testObjectConnector.read()
      this
    }

    override def process(): FactoryWithConnectorDeliveryVariable.this.type = this

    override def write(): FactoryWithConnectorDeliveryVariable.this.type = this

    override def get(): Dataset[TestObject] = {
      val spark = SparkSession.getActiveSession.get
      import spark.implicits._
      output.as[TestObject]
    }
  }

  class FactoryWithConnectorDeliveryMethod extends Factory[Dataset[TestObject]] {

    var testObjectConnector: Connector = _

    @Delivery(id = "1")
    def deliveryConnector(c: CSVConnector): Unit = {
      testObjectConnector = c
    }


    var output: DataFrame = _

    override def read(): FactoryWithConnectorDeliveryMethod.this.type = {
      output = testObjectConnector.read()
      this
    }

    override def process(): FactoryWithConnectorDeliveryMethod.this.type = this

    override def write(): FactoryWithConnectorDeliveryMethod.this.type = this

    override def get(): Dataset[TestObject] = {
      val spark = SparkSession.getActiveSession.get
      import spark.implicits._
      output.as[TestObject]
    }
  }

  class FactoryWithConnectorException extends Factory[Dataset[TestObject]] {

    @Delivery(id = "1")
    private[this] val deliveryConnector = Connector.empty
    private[this] var testObjectConnector = Connector.empty

    @Delivery(id = "1") def deliveryConnector(c: Connector): Unit = {
      testObjectConnector = c
    }

    @Delivery(optional = true) def deliveryConnector(s: FileConnector): Unit = println("wrong method")

    var output: DataFrame = _

    override def read(): FactoryWithConnectorException.this.type = {
      output = testObjectConnector.read()
      this
    }

    override def process(): FactoryWithConnectorException.this.type = this

    override def write(): FactoryWithConnectorException.this.type = this

    override def get(): Dataset[TestObject] = {
      val spark = SparkSession.getActiveSession.get
      import spark.implicits._
      output.as[TestObject]
    }
  }

}
