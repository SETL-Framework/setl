package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}
import com.jcdecaux.datacorp.spark.{DCContext, SparkSessionBuilder}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class SimplePipelineOptimizerSuite extends FunSuite {

  test("PipelineOptimizer should optimize a pipeline") {
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
    val dsFactory4 = new DatasetFactory4(spark)
    val pipeline = new Pipeline

    val stage0 = new Stage().addFactory(productFactory)
    val stage1 = new Stage().addFactory(dsFactory)
    val stage2 = new Stage().addFactory(dsFactory2)
    val stage3 = new Stage().addFactory(dsFactory3)
    val stage4 = new Stage().addFactory(dsFactory4)

    pipeline
      .optimization(true)
      .setInput(new Deliverable[String]("wrong_id_of_product1"))
      .setInput[String]("id_of_product1", productFactory.getClass)
      .setInput(ds2)
      .addStage(stage0)
      .addStage(stage1)
      .addStage(stage2)
      .addStage(stage3)
      .addStage(stage4)
      .describe()
      .run()

    pipeline.stages.foreach(_.factories.foreach(println))
    pipeline.deliverableDispatcher.deliveries.foreach(println)

    dsFactory3.get().show()
    assert(dsFactory3.get().count() === 4)
    assert(dsFactory3.get().filter($"x" === "pd1").count() === 2)
    assert(dsFactory3.get().filter($"x" === "id_of_product1").count() === 2)
    assert(dsFactory3.get().filter($"x" === "id_of_product1").collect().head === Product2("id_of_product1", "c2"))
    assert(dsFactory4.get() === 2)

    // Check inspector
    assert(pipeline.pipelineInspector.nodes.size === 5)
    assert(pipeline.pipelineInspector.flows.size === 8)
  }

  test("Pipeline optimise should opt") {
    System.clearProperty("app.environment")
    val context = DCContext.builder().withDefaultConfigLoader("myconf.conf").getOrCreate()
    import SimplePipelineOptimizerSuite._

    val pipeline = context
      .newPipeline()
      .addStage(classOf[F1])
      .addStage(classOf[F2])
      .addStage(classOf[F3])
      .addStage(classOf[F4])
      .addStage(classOf[F4a])
      .addStage(classOf[F4b])
      .addStage(classOf[F5])
    pipeline.describe()
    assert(pipeline.stages.size === 7)
    assert(pipeline.stages.head.factories.length === 1)

    val pipelineOpt = context
      .newPipeline()
      .addStage(classOf[F1])
      .addStage(classOf[F2])
      .addStage(classOf[F3])
      .addStage(classOf[F4])
      .addStage(classOf[F4a])
      .addStage(classOf[F4b])
      .addStage(classOf[F5])
      .optimization(true)

    pipelineOpt.describe()
    assert(pipelineOpt.stages.size === 2)
    assert(pipelineOpt.stages.head.factories.length === 4)

  }
}

object SimplePipelineOptimizerSuite {

  class F1 extends Factory[String] {
    val output = "output F1"

    override def read(): F1.this.type = this

    override def process(): F1.this.type = this

    override def write(): F1.this.type = {
      println(output)
      this
    }

    override def get(): String = output
  }

  class F2 extends Factory[Int] {
    val output = 111

    override def read(): F2.this.type = this

    override def process(): F2.this.type = this

    override def write(): F2.this.type = {
      println(output)
      this
    }

    override def get(): Int = output
  }

  class F3 extends Factory[Array[Int]] {

    @Delivery
    var input: Int = _
    var output: Array[Int] = _

    override def read(): F3.this.type = this

    override def process(): F3.this.type = {
      output = Array(input)
      this
    }

    override def write(): F3.this.type = {
      println(output)
      this
    }

    override def get(): Array[Int] = output
  }

  class F4 extends Factory[Double] {

    override def read(): F4.this.type = this

    override def process(): F4.this.type = this

    override def write(): F4.this.type = {
      println("output of F4")
      this
    }

    override def get(): Double = 111D
  }

  class F4a extends Factory[Double] {

    override def read(): F4a.this.type = this

    override def process(): F4a.this.type = this

    override def write(): F4a.this.type = {
      println("output of F4")
      this
    }

    override def get(): Double = 111D
  }

  class F4b extends Factory[Double] {

    override def read(): F4b.this.type = this

    override def process(): F4b.this.type = this

    override def write(): F4b.this.type = {
      println("output of F4")
      this
    }

    override def get(): Double = 111D
  }

  class F5 extends Factory[Set[Double]] {

    override def read(): F5.this.type = this

    override def process(): F5.this.type = this

    override def write(): F5.this.type = {
      println("output of F5")
      this
    }

    override def get(): Set[Double] = Set(1D, 2D)
  }


}
