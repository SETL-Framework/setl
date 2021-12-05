package io.github.setl.workflow

import io.github.setl.Setl
import io.github.setl.annotation.Delivery
import io.github.setl.transformation.Factory
import io.github.setl.workflow.PipelineInspectorSuite.{LastFactory, MultipleInputFactory, WrongLastFactory}
import org.scalatest.funsuite.AnyFunSuite

class PipelineInspectorSuite extends AnyFunSuite {

  test("Pipeline inspector should handle multiple same-type-delivery with different IDs") {

    val context: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .setSparkMaster("local")
      .getOrCreate()


    val pipeline = context
      .newPipeline()
      .setInput("string_1", "1")
      .setInput("string_2", "2")
      .addStage[MultipleInputFactory]()
      .addStage[LastFactory]()

    val pipelineInspector = new PipelineInspector(pipeline).inspect()
    pipelineInspector.describe()
    println(pipelineInspector.getDataFlowGraph.toDiagram)
    assertThrows[NotImplementedError](pipelineInspector.getDataFlowGraph.diagramId)

    pipeline.run()

    assert(pipelineInspector.nodes.size === 2)
    assert(pipelineInspector.flows.size === 3)

    assert(pipelineInspector.flows.exists(_.deliveryId == "1"))
    assert(pipelineInspector.flows.exists(_.deliveryId == "2"))
    assert(pipelineInspector.flows.exists(_.deliveryId == "MultipleInputFactory"))
  }

  test("Exception should be thrown when delivery id mis-matches") {
    val context: Setl = Setl.builder()
      .withDefaultConfigLoader()
      .setSparkMaster("local")
      .getOrCreate()

    val pipeline = context
      .newPipeline()
      .setInput("string_1", "1")
      .setInput("string_2", "2")
      .addStage[MultipleInputFactory]()
      .addStage[WrongLastFactory]()
      .describe()

    assertThrows[IllegalArgumentException](pipeline.run())
  }
}

object PipelineInspectorSuite {

  class MultipleInputFactory extends Factory[String] {

    override def deliveryId: String = "MultipleInputFactory"

    @Delivery(id = "1") var str1: String = _
    @Delivery(id = "2") var str2: String = _

    var output: String = ""

    override def read(): MultipleInputFactory.this.type = this

    override def process(): MultipleInputFactory.this.type = {
      output = "haha" + str1 + str2
      this
    }

    override def write(): MultipleInputFactory.this.type = {
      println(output)
      this
    }

    override def get(): String = output
  }

  class LastFactory extends Factory[String] {

    @Delivery(id = "MultipleInputFactory") var string: String = _

    override def read(): LastFactory.this.type = this

    override def process(): LastFactory.this.type = this

    override def write(): LastFactory.this.type = {
      println(string)
      this
    }

    override def get(): String = string
  }

  class WrongLastFactory extends Factory[String] {

    @Delivery() var string: String = _

    override def read(): WrongLastFactory.this.type = this

    override def process(): WrongLastFactory.this.type = this

    override def write(): WrongLastFactory.this.type = {
      println(string)
      this
    }

    override def get(): String = string
  }

}
