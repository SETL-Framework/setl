package io.github.setl.workflow

import io.github.setl.annotation.Delivery
import io.github.setl.transformation.{Factory, FactoryOutput}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe

class FlowSuite extends AnyFunSuite {

  import FlowSuite._

  val nodeProductFactory = new Node(new ProductFactory, 0, false)
  val nodeProduct2Factory = new Node(new Product2Factory, 1, false)

  test("Flow constructor") {
    val flow = Flow(nodeProductFactory, nodeProduct2Factory)

    assert(flow.deliveryId === "")
    assert(flow.stage === 0)
    assert(flow.payload === universe.typeOf[Product1])
  }

  test("Flow should generate diagram") {
    val flow = Flow(nodeProductFactory, nodeProduct2Factory)

    val externalNode = External.NODE.copy(output = FactoryOutput(universe.typeOf[String], Seq.empty, "", external = true))
    val flowExternal = Flow(externalNode, nodeProductFactory)

    val expectedDiagram =
      """Product2Factory <|-- Product1 : Input""".stripMargin.replace(" ", "")

    val expectedExternalFlowDiagram = "ProductFactory <|-- StringExternal : Input".replace(" ", "")

    assert(flow.diagramId === "")
    assert(flow.toDiagram.replace(" ", "") === expectedDiagram)
    assert(flowExternal.toDiagram.replace(" ", "") === expectedExternalFlowDiagram)
  }

}

object FlowSuite {


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

  class Product2Factory extends Factory[Product2] {

    @Delivery
    val input: Product1 = null
    var output: Product2 = _

    override def read(): this.type = this

    override def process(): this.type = {
      this
    }

    override def write(): this.type = this

    override def get(): Product2 = output
  }

}
