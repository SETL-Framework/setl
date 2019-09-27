package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

import com.jcdecaux.datacorp.spark.exception.InvalidDeliveryException
import com.jcdecaux.datacorp.spark.transformation.{Factory, FactoryDeliveryMetadata, FactoryOutput}
import org.scalatest.FunSuite

import scala.reflect.runtime

object NodeSuite {

  abstract class Producer1 extends Factory[Product1]

  abstract class Producer2 extends Factory[Product2]

  abstract class Producer23 extends Factory[Product23]

  abstract class ProducerContainer1 extends Factory[Container[Product1]]

  abstract class ProducerContainer2 extends Factory[Container[Product2]]

  abstract class ProducerContainer23 extends Factory[Container[Product23]]

}

class NodeSuite extends FunSuite {

  import NodeSuite._

  val uuid1: UUID = UUID.randomUUID()
  val uuid2: UUID = UUID.randomUUID()
  val uuid3: UUID = UUID.randomUUID()

  val testMetadata = FactoryDeliveryMetadata(
    UUID.randomUUID(),
    name = "setterMethodName",
    argTypes = List(runtime.universe.typeOf[Int]),
    producer = null,
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

    assert(node1WithConsumer.targetNode(node21) === true)
    assert(node1WithConsumer.targetNode(node21WithWrongType) === false)
    assert(node1WithConsumer.targetNode(node21WithWrongProducer) === false)

    assert(node1.targetNode(node22) === true)
    assert(node1WithConsumer.targetNode(node22) === true)
    assert(node1WithWrongConsumer.targetNode(node22) === false)

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

}
