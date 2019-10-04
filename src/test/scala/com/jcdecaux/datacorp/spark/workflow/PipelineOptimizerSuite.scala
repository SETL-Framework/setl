package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.transformation.Deliverable
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class PipelineOptimizerSuite extends FunSuite {

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
}

object PipelineOptimizerSuite {


}
