package io.github.setl.workflow

import java.util.UUID

import io.github.setl.annotation.Delivery
import io.github.setl.exception.InvalidDeliveryException
import io.github.setl.storage.connector.Connector
import io.github.setl.storage.repository.SparkRepository
import io.github.setl.transformation.{Factory, FactoryDeliveryMetadata, FactoryOutput}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime

object NodeSuite {

  case class ComplexProduct(arg1: String, arg2: Option[Int], arg3: Seq[Product1])

  abstract class Producer1 extends Factory[Product1]

  abstract class Producer2 extends Factory[Product2]

  abstract class Producer23 extends Factory[Product23]

  abstract class ProducerContainer1 extends Factory[Container[Product1]]

  abstract class ProducerContainer2 extends Factory[Container[Product2]]

  abstract class ProducerContainer23 extends Factory[Container[Product23]]

  class ConcreteProducer1 extends Factory[Dataset[ComplexProduct]] {

    @Delivery
    private[this] val input1: String = ""

    @Delivery
    private[this] val input2: Array[Int] = Array.empty

    /** Read data */
    override def read(): ConcreteProducer1.this.type = this

    /** Process data */
    override def process(): ConcreteProducer1.this.type = this

    /** Write data */
    override def write(): ConcreteProducer1.this.type = this

    /** Get the processed data */
    override def get(): Dataset[ComplexProduct] = null
  }

}

class NodeSuite extends AnyFunSuite {

  import NodeSuite._

  val uuid1: UUID = UUID.randomUUID()
  val uuid2: UUID = UUID.randomUUID()
  val uuid3: UUID = UUID.randomUUID()

  val testMetadata: FactoryDeliveryMetadata = FactoryDeliveryMetadata(
    UUID.randomUUID(),
    factoryClass = classOf[Producer1],
    symbol = null,
    deliverySetter = null,
    argTypes = List(runtime.universe.typeOf[Int]),
    producer = classOf[External],
    optional = false
  )

  test("Test Node.targetNode with classInfo") {
    val nodeStage0 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Producer2]))
    )

    val nodeStage1 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val nodeStage1WithWrongClassInfo = Node(
      factoryClass = classOf[Producer23],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    assert(nodeStage0.targetNode(nodeStage1) === true)
    assert(nodeStage0.targetNode(nodeStage1WithWrongClassInfo) === false)
  }

  test("Test Node.targetNode with different stage") {

    val nodeStage0 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val nodeStage1 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val sameNodeStage0 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 0,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    assert(nodeStage0.targetNode(nodeStage1) === true)
    assert(nodeStage0.targetNode(sameNodeStage0) === false)
  }

  test("Test Node.targetNode with UUID") {
    val node1 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node2 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node3 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid1,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    assert(node1.targetNode(node2) === true)
    assert(node1.targetNode(node3) === false)
    assert(node1.targetNode(node1) === false)
  }

  test("Test node with input and output") {

    val node1 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node1WithConsumer = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Producer2]))
    )

    val node1WithWrongConsumer = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[ProducerContainer2]))
    )

    val node2 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[External]),
        testMetadata.copy(producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node21 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[External]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node21WithWrongType = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[Float]), producer = classOf[External]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node21WithWrongProducer = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[ProducerContainer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node22 = Node(
      factoryClass = classOf[Producer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[Producer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    assert(node1.targetNode(node21) === true)
    assert(node1.targetNode(node21WithWrongType) === false, "The Int input in the second node doesn't have the right producer")
    node1.showDiagram()
    assert(node1.listInputProducers === List(classOf[External]))

    assert(node1WithConsumer.targetNode(node21) === true)
    assert(node1WithConsumer.targetNode(node21WithWrongType) === false)
    assert(node1WithConsumer.targetNode(node21WithWrongProducer) === false)
    assert(node1WithConsumer.listInputProducers === List(classOf[External]))

    assert(node1.targetNode(node22) === true)
    assert(node1WithConsumer.targetNode(node22) === true)
    assert(node1WithWrongConsumer.targetNode(node22) === false)
    assert(node1WithWrongConsumer.listInputProducers === List(classOf[External]))

    assertThrows[InvalidDeliveryException](node1.targetNode(node2))
    assertThrows[InvalidDeliveryException](node1WithConsumer.targetNode(node2))

  }

  test("Test Node class erasure") {
    val node1 = Node(
      factoryClass = classOf[ProducerContainer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node11 = Node(
      factoryClass = classOf[ProducerContainer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[ProducerContainer2]))
    )

    val node2 = Node(
      factoryClass = classOf[ProducerContainer2],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[ProducerContainer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node3 = Node(
      factoryClass = classOf[ProducerContainer23],
      factoryUUID = uuid3,
      stage = 1,
      setters = List(
        testMetadata.copy(producer = classOf[ProducerContainer1]),
        testMetadata.copy(producer = classOf[ProducerContainer2])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    val node3String = Node(
      factoryClass = classOf[ProducerContainer23],
      factoryUUID = uuid3,
      stage = 1,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[Double]), producer = classOf[ProducerContainer1])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Producer23]))
    )

    assert(node1.targetNode(node2) === true)
    assert(node1.targetNode(node3) === true)

    assert(node11.targetNode(node2) === true)
    assert(node11.targetNode(node3) === false)
    assert(node11.targetNode(node3String) === false)
  }

  test("[SETL-25] Node should handle multiple input of the same type with different delivery id") {

    val node1 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[String]),
          id = "id_1"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[Producer1], Seq.empty, "id_2")
    )

    val node1Bis = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[String]),
          id = "id_1"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[Producer1], Seq.empty, "id_wrong")
    )

    val node1Ter = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[String]),
          id = "id_1"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[Producer1], Seq.empty, "nothing")
    )

    val node2 = Node(
      factoryClass = classOf[ProducerContainer1],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[Producer1]),
          id = "id_2"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[ProducerContainer1], Seq.empty, "id_3")
    )

    val node2Bis = Node(
      factoryClass = classOf[ProducerContainer1],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[Producer1]),
          id = "id_wrong"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[ProducerContainer1], Seq.empty, "id_3")
    )

    val node2Ter = Node(
      factoryClass = classOf[ProducerContainer1],
      factoryUUID = uuid2,
      stage = 1,
      setters = List(
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[Producer1]),
          id = "id_wrong"
        ),
        testMetadata.copy(
          argTypes = List(runtime.universe.typeOf[Producer1]),
          id = "id_2"
        )
      ),
      output = FactoryOutput(runtime.universe.typeOf[ProducerContainer1], Seq.empty, "id_3")
    )

    assert(node1.targetNode(node2))
    assert(node1Bis.targetNode(node2Bis))

    assert(!node1.targetNode(node2Bis))
    assert(!node1Bis.targetNode(node2))

    assert(node1.targetNode(node2Ter))
    assert(node1Bis.targetNode(node2Ter))
    assert(!node1Ter.targetNode(node2Ter))

  }

  test("Node should be able to generate a Mermaid diagram string") {
    val factory = new ConcreteProducer1
    val node = new Node(factory, 0, true)

    val expectedOutput = """class ConcreteProducer1 {
                           |  <<Factory[Dataset[ComplexProduct]]>>
                           |  +String
                           |  +Array[Int]
                           |}
                           |
                           |class DatasetComplexProductFinal {
                           |  <<Dataset[ComplexProduct]>>
                           |  >arg1: String
                           |  >arg2: Option[Int]
                           |  >arg3: Seq[Product1]
                           |}
                           |
                           |DatasetComplexProductFinal <|.. ConcreteProducer1 : Output
                           |""".stripMargin

    assert(
      node.toDiagram.replaceAll("\n|\r\n|\r", System.getProperty("line.separator")) ===
      expectedOutput.replaceAll("\n|\r\n|\r", System.getProperty("line.separator"))
    )
  }

  test("Node should be able to generate a Mermaid diagram without throwing errors") {
    val node = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[SparkRepository[String]], List())
    )

    println(node.toDiagram)

    val node2 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[Connector], List())
    )

    println(node2.toDiagram)

    val node3 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[ComplexProduct], List())
    )

    println(node3.toDiagram)

    val node4 = Node(
      factoryClass = classOf[Producer1],
      factoryUUID = uuid1,
      stage = 0,
      setters = List(
        testMetadata.copy(argTypes = List(runtime.universe.typeOf[String]), producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[String], List())
    )

    println(node4.toDiagram)
  }

}
