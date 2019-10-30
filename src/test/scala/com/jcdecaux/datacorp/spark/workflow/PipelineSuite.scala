package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

import scala.reflect.runtime.{universe => ru}

//////////////////////
// TESTS START HERE //
//////////////////////
class PipelineSuite extends FunSuite {

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
    assert(pipeline.deliverableDispatcher.deliveries.length === 5)
    assert(pipeline.getDeliverable(ru.typeOf[Container2[Product2]]).head.get == Container2(Product2("a", "b")))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 4)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "Container2Factory").get.input.length === 2)
    assert(pipeline.pipelineInspector.flows.size === 4)

  }

  test("Test Dataset pipeline") {
    val spark = new SparkSessionBuilder("test").setEnv("local").getOrCreate()
    import spark.implicits._

    val ds2: Dataset[Product2] = Seq(
      Product2("id_of_product1", "c2"),
      Product2("pd1", "c2")
    ).toDS

    val f1 = new ProductFactory
    val f2 = new DatasetFactory(spark)
    val f3 = new DatasetFactory2(spark)
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

    f3.get().show()
    assert(f3.get().count() === 2)
    assert(f3.get().filter($"x" === "pd1").count() === 1)
    assert(f3.get().filter($"x" === "id_of_product1").count() === 1)
    assert(f3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2"))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 5)
  }

  test("Pipeline should handle addStage[T](args, persist)") {
    val spark = new SparkSessionBuilder("test").setEnv("local").getOrCreate()
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
    assert(f3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2"))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 5)
  }

  test("Test get methods of Pipeline") {
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

    // Test get() method
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].count() === 2)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "pd1").count() === 1)
    assert(pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").count() === 1)
    assert(
      pipeline.getLastOutput.asInstanceOf[Dataset[Product2]].filter($"x" === "id_of_product1").collect().head ===
        Product2("id_of_product1", "c2")
    )

    // Test get[A](cls) method
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).count() === 2)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "id_of_product1").count() === 1)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "pd1").count() === 1)
    assert(
      pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2])
        .filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2")
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
        Product2("id_of_product1", "c2")
    )

    // Test get[A](cls)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).count() === 2)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "id_of_product1").count() === 1)
    assert(pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2]).filter($"x" === "pd1").count() === 1)
    assert(
      pipeline.getOutput[Dataset[Product2]](classOf[DatasetFactory2])
        .filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2")
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

    val spark = new SparkSessionBuilder("test").setEnv("local").getOrCreate()
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
    assert(dsFactory3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2"))

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 4)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory2").get.input.length === 3)
    assert(pipeline.pipelineInspector.nodes.find(_.getPrettyName === "DatasetFactory3").get.input.length === 3)
    assert(pipeline.pipelineInspector.flows.size === 7)
  }

  test("Pipeline should be able to describe with empty flow and node") {
    val pipeline = new Pipeline
    pipeline.describe()
  }
}

object PipelineSuite {

  abstract class Producer1 extends Factory[External]

  abstract class Producer2 extends Factory[External]

  class ProductFactory extends Factory[Product1] {
    @Delivery
    var id: String = _
    var output: Product1 = _

    override def read(): ProductFactory.this.type = this

    override def process(): ProductFactory.this.type = {
      output = Product1(id)
      this
    }

    override def write(): ProductFactory.this.type = this

    override def get(): Product1 = output
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
      output = ds.join(ds2, Seq("x")).as[Product2]
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

}