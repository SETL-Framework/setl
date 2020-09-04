package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.exception.AlreadyExistsException
import com.jcdecaux.setl.storage.SparkRepositoryBuilder
import com.jcdecaux.setl.storage.connector.FileConnector
import com.jcdecaux.setl.transformation.{Deliverable, Factory}
import com.jcdecaux.setl.workflow.DeliverableDispatcherSuite.FactoryWithMultipleAutoLoad
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.{universe => ru}

//////////////////////
// TESTS START HERE //
//////////////////////
class PipelineSuite extends AnyFunSuite with Matchers {

  import PipelineSuite._

  test("Test pipeline") {

    val f1 = new ProductFactory
    val f2 = new Product2Factory
    val f3 = new ContainerFactory
    val f4 = new Container2Factory

    val pipeline = new Pipeline

    val stage1 = new Stage().addFactory(f1).addFactory(f2)
    val stage2 = new Stage().addFactory(f3)
    val stage3 = new Stage().addFactory(f4)

    pipeline
      .setInput(new Deliverable[String]("id_of_product1"))
      .addStage(stage1)
      .addStage(stage2)
      .addStage(stage3)
      .describe()
      .run()

    //    pipeline.dispatchManagers.deliveries.foreach(x => println(x.get))
    assert(pipeline.deliverableDispatcher.getRegistryLength === 5)
    assert(pipeline.getDeliverable(ru.typeOf[Container2[Product2]]).head.getPayload == Container2(Product2("a", "b")))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 4)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "Container2Factory").get.input.length === 2)
    assert(pipeline.pipelineInspector.flows.size === 4)
    assert(f3.get().content.x === "id_of_product1")

  }

  test("Test addStage with primitive types arguments") {
    val bool: Boolean = true
    val byte: Byte = 0.toByte
    val char: Char = 0.toChar
    val short: Short = 0.toShort
    val int: Int = 0
    val long: Long = 0L
    val float: Float = 0F
    val double: Double = 0D
    val string: String = "string"
    val product2: Product2 = Product2("x", "y")
    val inputString: String = "payload"

    val pipeline = new Pipeline
    pipeline
      .optimization(true)
      .setInput[String](inputString, classOf[TestFactory], "id")
      .addStage[TestFactory](Array(
        bool,
        byte,
        char,
        short,
        int,
        long,
        float,
        double,
        bool,
        string,
        product2
    ))
      .run()

    assert(pipeline.optimization)

    val lastOutput = pipeline.getLastOutput.asInstanceOf[TestFactoryArgs]
    assert(lastOutput.bool == bool)
    assert(lastOutput.byte == byte)
    assert(lastOutput.char == char)
    assert(lastOutput.short == short)
    assert(lastOutput.int == int)
    assert(lastOutput.long == long)
    assert(lastOutput.float == float)
    assert(lastOutput.double == double)
    assert(lastOutput.bool == bool)
    assert(lastOutput.string == string)
    assert(lastOutput.product2 == product2)
    assert(lastOutput.inputString == inputString)

    assert(pipeline.getOutput[TestFactoryArgs](classOf[TestFactory]) == lastOutput)
    assertThrows[NoSuchElementException](pipeline.getOutput(classOf[ProductFactory]))

    val pipeline2 = new Pipeline
    pipeline2
      .optimization(false)
      .setInput[String]("payload", classOf[TestFactory], "id")
      .addStage(classOf[TestFactory],
          bool,
          byte,
          char,
          short,
          int,
          long,
          float,
          double,
          bool,
          string,
          product2
      )
      .run()

    assert(!pipeline2.optimization)

    val lastOutput2 = pipeline2.getLastOutput.asInstanceOf[TestFactoryArgs]
    assert(lastOutput2.bool == bool)
    assert(lastOutput2.byte == byte)
    assert(lastOutput2.char == char)
    assert(lastOutput2.short == short)
    assert(lastOutput2.int == int)
    assert(lastOutput2.long == long)
    assert(lastOutput2.float == float)
    assert(lastOutput2.double == double)
    assert(lastOutput2.bool == bool)
    assert(lastOutput2.string == string)
    assert(lastOutput2.product2 == product2)
    assert(lastOutput2.inputString == inputString)

    assert(pipeline2.getOutput[TestFactoryArgs](classOf[TestFactory]) == lastOutput)
    assertThrows[NoSuchElementException](pipeline2.getOutput(classOf[ProductFactory]))
  }

  test("Check deliveries before running pipeline") {
    val spark = new SparkSessionBuilder("test").setEnv("local").setSparkMaster("local").getOrCreate()
    val errorMessage = "requirement failed"

    // 1) Missing input Delivery in the first Factory
    val pipeline = new Pipeline()
      .addStage[DatasetFactory](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline.run()).getMessage == errorMessage)

    // 2) No missing input Delivery in the first Factory
    new Pipeline()
      .setInput[Product1](Product1("product1"))
      .addStage[DatasetFactory](Array(spark))
      .run()

    // 3) Missing input and factory output Delivery in the second Factory
    val pipeline3 = new Pipeline()
      .addStage[Product2Factory]()
      .addStage[DatasetFactory](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline3.run()).getMessage == errorMessage)

    // 4) No missing input Delivery in the second Factory
    new Pipeline()
      .setInput[Product1](Product1("good product1"))
      .addStage[Product2Factory]()
      .addStage[DatasetFactory](Array(spark))
      .run()

    // 5) No missing factory output Delivery in the second Factory
    new Pipeline()
      .setInput[String]("good string")
      .addStage[ProductFactory]()
      .addStage[DatasetFactory](Array(spark))
      .run()

    // 6) Missing input Delivery in the first Factory because of deliveryId
    val pipeline6 = new Pipeline()
      .setInput[Product1](Product1("good product1"), deliveryId = "goodId")
      .addStage[DatasetFactory](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline6.run()).getMessage == errorMessage)

    // 7) Missing input Delivery in the second Factory because of deliveryId
    // The first Factory is not even run
    val pipeline7 = new Pipeline()
      .setInput[Product1](Product1("good product1"), deliveryId = "goodId")
      .addStage[Product2Factory]()
      .addStage[DatasetFactory](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline7.run()).getMessage == errorMessage)

    // 8) No missing factory output Delivery in the second factory of the second Stage
    val stage = new Stage()
    stage.addFactory(classOf[Product2Factory]).addFactory(classOf[DatasetFactory], spark).parallel(false)
    new Pipeline()
      .setInput[String]("good string")
      .addStage[ProductFactory]()
      .addStage(stage)
      .run()

    // 9) Missing input Delivery due to wrong producer
    val pipeline9 = new Pipeline()
      .setInput[Product1](Product1("wrong product1"))
      .addStage[DatasetFactoryBis](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline9.run()).getMessage == errorMessage)
    val pipeline9bis = new Pipeline()
      .setInput[String]("wrong product1")
      .addStage[ProductFactory]()
      .addStage[DatasetFactoryBis](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline9bis.run()).getMessage == errorMessage)

    // 10) No missing factory output Delivery
    new Pipeline()
      .setInput[String]("good product1")
      .addStage[ProductFactoryBis]()
      .addStage[DatasetFactoryBis](Array(spark))
      .run()

    // 11) Missing input Delivery due to wrong consumer
    val pipeline11 = new Pipeline()
      .setInput[String]("wrong consumer", consumer = classOf[DatasetFactory])
      .addStage[ProductFactory]()
    assert(the[IllegalArgumentException].thrownBy(pipeline11.run()).getMessage == errorMessage)

    // 12 No missing input Delivery due to wrong consumer
    new Pipeline()
      .setInput[String]("good consumer", consumer = classOf[ProductFactory])
      .addStage[ProductFactory]()
      .run()
    new Pipeline()
      .setInput[String]("still a good consumer")
      .addStage[ProductFactory]()
      .run()

    // 13) Missing factory output due to wrong consumer
    val pipeline13 = new Pipeline()
      .setInput[String]("test")
      .addStage[ProductFactoryBis]()
      .addStage[DatasetFactoryBis2](Array(spark))
    assert(the[IllegalArgumentException].thrownBy(pipeline13.run()).getMessage == errorMessage)
  }

  test("Test Dataset pipeline") {
    val spark = new SparkSessionBuilder("test").setEnv("local").setSparkMaster("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val f1 = new ProductFactory
    val f2 = new DatasetFactory(spark)
    val f3 = new DatasetFactory2(spark)

    assert(f1.writable)
    f1.writable(false)
    assert(!f1.writable)
    f1.writable(true)

    val pipeline = new Pipeline

    val stage0 = new Stage().addFactory(f1)
    val stage1 = new Stage().addFactory(f2)
    val stage2 = new Stage().addFactory(f3)

    pipeline
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", f1.getClass)
      .setInput(ds2)
      .addStage(stage0)
      .addStage(stage1)
      .addStage(stage2)
      .describe()
      .run()

    assert(pipeline.getStage(0).contains(stage0))
    assert(pipeline.getStage(1).contains(stage1))
    assert(pipeline.getStage(2).contains(stage2))

    pipeline.toDiagram
    assertThrows[NotImplementedError](pipeline.diagramId)

    f3.get().show()
    assert(f3.get().count() === 2)
    assert(f3.get().filter($"x" === "pd1").collect().head === Product2("pd1", "c2"))
    assert(f3.get().filter($"x" === "id_of_product1").count() === 1)
    assert(f3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "produced_by_datasetFactory2"))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 5)
  }

  test("Pipeline should handle addStage[T](args, persist)") {
    val spark = new SparkSessionBuilder("test").setEnv("local").setSparkMaster("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val pipeline = new Pipeline

    pipeline
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", classOf[ProductFactory])
      .setInput(ds2)
      .addStage[ProductFactory]()
      .addStage[DatasetFactory](Array(spark))
      .addStage[DatasetFactory2](Array(spark))
      .describe()
      .run()

    val f3 = pipeline.stages.last.factories.head.asInstanceOf[DatasetFactory2]

    f3.get().show()
    assert(f3.get().count() === 2)
    assert(f3.get().filter($"x" === "pd1").count() === 1)
    assert(f3.get().filter($"x" === "id_of_product1").count() === 1)

    assert(
      f3.get().filter($"x" === "id_of_product1").collect().head ===
        Product2("id_of_product1", "produced_by_datasetFactory2")
    )

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 5)
  }

  test("Test get methods of Pipeline") {
    val spark: SparkSession = new SparkSessionBuilder("test").setEnv("local").setSparkMaster("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val pipeline = new Pipeline

    pipeline
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", classOf[ProductFactory])
      .setInput(ds2)
      .addStage(classOf[ProductFactory])
      .addStage(classOf[DatasetFactory], spark)
      .addStage(classOf[DatasetFactory2], spark)
      .describe()
      .run()

    pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].show()

    // Test get() method
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].count() === 2)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "pd1").count() === 1)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").count() === 1)
    assert(
      pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").collect().head ===
        Product2("id_of_product1", "produced_by_datasetFactory2")
    )

    // Test get[A](cls) method
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).count() === 2)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "id_of_product1").count() === 1)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "pd1").count() === 1)
    assert(
      pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2])
        .filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "produced_by_datasetFactory2")
    )
  }

  test("Test Pipeline creation with default primary constructor") {
    val spark: SparkSession = new SparkSessionBuilder("test").setEnv("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val pipeline = new Pipeline

    pipeline
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", classOf[ProductFactory])
      .setInput(ds2)
      .addStage(classOf[ProductFactory])
      .addStage(classOf[DatasetFactory], spark)
      .addStage(classOf[DatasetFactory2], spark)
      .describe()
      .run()

    pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].show()

    // Test get()
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].count() === 2)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "pd1").count() === 1)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").count() === 1)
    assert(
      pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").collect().head ===
        Product2("id_of_product1", "produced_by_datasetFactory2")
    )

    // Test get[A](cls)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).count() === 2)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "id_of_product1").count() === 1)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "pd1").count() === 1)
    assert(
      pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2])
        .filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "produced_by_datasetFactory2")
    )

    // Test inspector
    assert(pipeline.pipelineInspector.nodes.size === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 5)

    // Test throwing exception
    assertThrows[IllegalArgumentException](
      pipeline.addStage(classOf[DatasetFactory], spark, "sqdfsd"),
      "IllegalArgumentException should be thrown as the number of constructor argument is wrong"
    )
  }

  test("Pipeline exceptions") {

    val f1 = new ProductFactory
    val pipeline = new Pipeline

    val stage0 = new Stage().addFactory(f1)

    val pipeline2 = new Pipeline()
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("wrong_id_of_product2", classOf[Producer1], classOf[Producer2])
      .setInput[String]("id_of_product1")
      .addStage(stage0)

    assertThrows[NoSuchElementException](pipeline2.run())

    val delivery = new Deliverable[String]("test")

    val pipeline3 = new Pipeline().setInput(delivery)

    assertThrows[AlreadyExistsException](pipeline3.setInput(delivery))
  }

  test("Test pipeline with two inputs of the same type") {

    val spark = new SparkSessionBuilder("test").setEnv("local").setSparkMaster("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val productFactory = new ProductFactory
    val dsFactory = new DatasetFactory(spark)
    val dsFactory2 = new DatasetFactory2(spark)
    val dsFactory3 = new DatasetFactory3(spark)
    val pipeline = new Pipeline

    val stage0 = new Stage().addFactory(productFactory)
    val stage1 = new Stage().addFactory(dsFactory)
    val stage2 = new Stage().addFactory(dsFactory2)
    val stage3 = new Stage().addFactory(dsFactory3)

    pipeline
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", productFactory.getClass)
      .setInput(ds2)
      .addStage(stage0)
      .addStage(stage1)
      .addStage(stage2)
      .addStage(stage3)
      .describe()
      .run()

    dsFactory3.get().show()
    assert(dsFactory3.get().count() === 4)
    assert(dsFactory3.get().filter($"x" === "pd1").count() === 2)
    assert(dsFactory3.get().filter($"x" === "id_of_product1").count() === 2)
    assert(dsFactory3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "produced_by_datasetFactory2"))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 4)
    assert(pipeline.pipelineInspector.flows.size === 8)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory3").get.input.length === 3)

    pipeline.showDiagram()
  }

  test("Pipeline should be able to describe with empty flow and node") {
    val pipeline = new Pipeline
    pipeline.describe()
  }

  test("[SETL-25] Pipeline should be able to delivery multiple autoLoad deliveries with same type but different id") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").setSparkMaster("local").build().get()
    import spark.implicits._

    val repo1 = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_multiple_auto_load_1")
      .getOrCreate()

    val repo2 = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_multiple_auto_load_2")
      .getOrCreate()

    val ds1 = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDS

    val ds2 = Seq(
      Product2("A", "11")
    ).toDS

    repo1.save(ds1)
    repo2.save(ds2)

    val factoryWithAutoLoad = new FactoryWithMultipleAutoLoad

    new Pipeline()
      .setInput(repo1, "delivery1", consumer = classOf[FactoryWithMultipleAutoLoad])
      .setInput(repo2, "delivery2")
      .addStage(factoryWithAutoLoad)
      .describe()
      .run()

    assert(factoryWithAutoLoad.input.count() === 3)
    assert(factoryWithAutoLoad.input2.count() === 1)

    repo1.getConnector.asInstanceOf[FileConnector].delete()
    repo2.getConnector.asInstanceOf[FileConnector].delete()
  }

  test("SETL-15: Pipeline should handle primitive type delivery") {
    val by: Byte = 0.toByte
    val byWrong: Byte = 0.toByte
    val s: Short = 1.toShort
    val i: Int = 2
    val l: Long = 3.toLong
    val f: Float = 4.toFloat
    val d: Double = 5.toDouble
    val bo: Boolean = true
    val c: Char = 6.toChar
    val str: String = "7"
    val strAry: Array[String] = Array("a", "b")
    val fltAry: Array[Float] = Array(99F, 11F)

    val fac = new PrimaryDeliveryFactory

    new Pipeline()
      .setInput(by, classOf[PrimaryDeliveryFactory], "byte")
      .setInput(byWrong, classOf[PrimaryDeliveryFactory])
      .setInput(s)
      .setInput(i)
      .setInput(l)
      .setInput(f)
      .setInput(d)
      .setInput(bo)
      .setInput(c)
      .setInput(str)
      .setInput(strAry)
      .setInput(fltAry)
      .addStage(fac)
      .run()
      .showDiagram()

    assert(fac.by === by)
    assert(fac.s === s)
    assert(fac.i === i)
    assert(fac.l === l)
    assert(fac.f === f)
    assert(fac.d === d)
    assert(fac.bo === bo)
    assert(fac.c === c)
    assert(fac.str equals str)
    fac.strArray should contain theSameElementsAs strAry
    fac.floatArray should contain theSameElementsAs fltAry
  }

}

object PipelineSuite {

  case class TestFactoryArgs(
                         bool: Boolean,
                         byte: Byte,
                         char: Char,
                         short: Short,
                         int: Int,
                         long: Long,
                         float: Float,
                         double: Double,
                         bool2: Boolean,
                         string: String,
                         product2: Product2,
                         inputString: String
                       )

  class TestFactory(
                          bool: Boolean,
                          byte: Byte,
                          char: Char,
                          short: Short,
                          int: Int,
                          long: Long,
                          float: Float,
                          double: Double,
                          bool2: Boolean,
                          string: String,
                          product2: Product2
                        ) extends Factory[TestFactoryArgs] {
    @Delivery(id = "id")
    private[this] val inputString: String = null
    private[this] var output: TestFactoryArgs = _

    override def read(): TestFactory.this.type = this

    override def process(): TestFactory.this.type = {
      output = TestFactoryArgs(bool, byte, char, short, int, long, float, double, bool2, string, product2, inputString)

      this
    }

    override def write(): TestFactory.this.type = this

    override def get(): TestFactoryArgs = output
  }

  abstract class Producer1 extends Factory[External]

  abstract class Producer2 extends Factory[External]

  class ProductFactory extends Factory[Product1] {
    @Delivery
    private[this] val id: String = null
    private[this] var output: Product1 = _

    override def read(): ProductFactory.this.type = this

    override def process(): ProductFactory.this.type = {
      output = Product1(id)
      this
    }

    override def write(): ProductFactory.this.type = this

    override def get(): Product1 = output
  }

  class ProductFactoryBis extends Factory[Product1] {
    @Delivery
    private[this] val id: String = null
    private[this] var output: Product1 = _

    override def read(): ProductFactoryBis.this.type = this

    override def process(): ProductFactoryBis.this.type = {
      output = Product1(id)
      this
    }

    override def write(): ProductFactoryBis.this.type = this

    override def get(): Product1 = output

    override def consumers: Seq[Class[_ <: Factory[_]]] = Seq(classOf[DatasetFactoryBis])
  }

  class Product2Factory extends Factory[Product2] {
    var output: Product2 = _

    override def read(): this.type = this

    override def process(): this.type = {
      output = Product2("a", "b")
      this
    }

    override def write(): this.type = this

    override def get(): Product2 = output
  }

  class ContainerFactory extends Factory[Container[Product1]] {
    @Delivery
    var product1: Product1 = _
    var output: Container[Product1] = _

    override def read(): ContainerFactory.this.type = this

    override def process(): ContainerFactory.this.type = {
      output = Container(product1)
      this
    }

    override def write(): ContainerFactory.this.type = this

    override def get(): Container[Product1] = output
  }

  class Container2Factory extends Factory[Container2[Product2]] {
    @Delivery
    var p1: Product1 = _
    var p2: Product2 = _
    var output: Container2[Product2] = _

    @Delivery
    def setProduct(v: Product2): this.type = {
      this.p2 = v
      this
    }

    override def read(): this.type = this

    override def process(): this.type = {
      output = Container2(p2)
      this
    }

    override def write(): this.type = this

    override def get(): Container2[Product2] = output
  }

  class DatasetFactory(spark: SparkSession) extends Factory[Dataset[Product1]] {

    import spark.implicits._

    @Delivery
    var p1: Product1 = _
    var output: Dataset[Product1] = _

    override def read(): DatasetFactory.this.type = this

    override def process(): DatasetFactory.this.type = {
      output = Seq(p1, Product1("pd1")).toDS
      this
    }

    override def write(): DatasetFactory.this.type = this

    override def get(): Dataset[Product1] = output
  }

  class DatasetFactoryBis(spark: SparkSession) extends Factory[Dataset[Product1]] {

    import spark.implicits._

    @Delivery(producer = classOf[ProductFactoryBis])
    var p1: Product1 = _
    var output: Dataset[Product1] = _

    override def read(): DatasetFactoryBis.this.type = this

    override def process(): DatasetFactoryBis.this.type = {
      output = Seq(p1, Product1("pd1")).toDS
      this
    }

    override def write(): DatasetFactoryBis.this.type = this

    override def get(): Dataset[Product1] = output
  }

  class DatasetFactoryBis2(spark: SparkSession) extends Factory[Dataset[Product1]] {

    import spark.implicits._

    @Delivery
    var p1: Product1 = _
    var output: Dataset[Product1] = _

    override def read(): DatasetFactoryBis2.this.type = this

    override def process(): DatasetFactoryBis2.this.type = {
      output = Seq(p1, Product1("pd1")).toDS
      this
    }

    override def write(): DatasetFactoryBis2.this.type = this

    override def get(): Dataset[Product1] = output
  }

  class DatasetFactory2(spark: SparkSession) extends Factory[Dataset[Product2]] {
    @Delivery
    var p1: Product1 = _
    @Delivery
    var ds: Dataset[Product1] = _
    @Delivery
    var ds2: Dataset[Product2] = _
    var output: Dataset[Product2] = _

    override def read(): DatasetFactory2.this.type = this

    override def process(): DatasetFactory2.this.type = {
      import spark.implicits._
      output = Seq(
        ds.withColumn("y", functions.lit("produced_by_datasetFactory2")).as[Product2].head,
        ds2.collect().last
      ).toDS()
      this
    }

    override def write(): DatasetFactory2.this.type = this

    override def get(): Dataset[Product2] = output
  }

  class DatasetFactory3(spark: SparkSession) extends Factory[Dataset[Product2]] {

    @Delivery
    var p1: Product1 = _

    @Delivery(producer = classOf[DatasetFactory2])
    var ds: Dataset[Product2] = _

    @Delivery
    var ds2: Dataset[Product2] = _

    var output: Dataset[Product2] = _

    override def read(): DatasetFactory3.this.type = this

    override def process(): DatasetFactory3.this.type = {
      output = ds.union(ds2)
      this
    }

    override def write(): DatasetFactory3.this.type = this

    override def get(): Dataset[Product2] = output
  }

  class DatasetFactory4(spark: SparkSession) extends Factory[Long] {

    @Delivery
    var ds1: Dataset[Product1] = _

    override def read(): DatasetFactory4.this.type = this

    override def process(): DatasetFactory4.this.type = {
      this
    }

    override def write(): DatasetFactory4.this.type = this

    override def get(): Long = ds1.count()
  }

  class PrimaryDeliveryFactory extends Factory[String] {

    @Delivery(id = "byte")
    var by: Byte = _
    @Delivery
    var s: Short = _
    @Delivery
    var i: Int = _
    @Delivery
    var l: Long = _
    @Delivery
    var f: Float = _
    @Delivery
    var d: Double = _
    @Delivery
    var bo: Boolean = _
    @Delivery
    var c: Char = _
    @Delivery
    var str: String = ""
    @Delivery
    val strArray: Array[String] = Array.empty
    @Delivery
    val floatArray: Array[Float] = Array.empty

    override def read(): PrimaryDeliveryFactory.this.type = this

    override def process(): PrimaryDeliveryFactory.this.type = this

    override def write(): PrimaryDeliveryFactory.this.type = this

    override def get(): String = "test"
  }

}
