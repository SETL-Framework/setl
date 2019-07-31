package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.exception.InvalidDeliveryException
import com.jcdecaux.datacorp.spark.transformation.{FactoryInput, FactoryOutput}
import org.scalatest.FunSuite

import scala.reflect.runtime

class NodeSuite extends FunSuite {

  test("Test Node.targetNode with classInfo") {
    val nodeStage0 = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Product2]))
    )

    val nodeStage1 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val nodeStage1WithWrongClassInfo = Node(
      factoryClass = classOf[Product23],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    assert(nodeStage0.targetNode(nodeStage1) === true)
    assert(nodeStage0.targetNode(nodeStage1WithWrongClassInfo) === false)
  }

  test("Test Node.targetNode with different stage") {

    val nodeStage0 = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val nodeStage1 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val sameNodeStage0 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 0,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    assert(nodeStage0.targetNode(nodeStage1) === true)
    assert(nodeStage0.targetNode(sameNodeStage0) === false)
  }

  test("Test Node.targetNode with UUID") {
    val node1 = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node2 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node3 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    assert(node1.targetNode(node2) === true)
    assert(node1.targetNode(node3) === false)
    assert(node1.targetNode(node1) === false)
  }

  test("Test node with input and output") {

    val node1 = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node1WithConsumer = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Product2]))
    )

    val node1WithWrongConsumer = Node(
      factoryClass = classOf[Product1],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Container[Product2]]))
    )

    val node2 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[External]),
        FactoryInput(runtime.universe.typeOf[Int], producer = classOf[External])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node21 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[External]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node21WithWrongType = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Float], classOf[External]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node21WithWrongProducer = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product1]]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node22 = Node(
      factoryClass = classOf[Product2],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Product1]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
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
      factoryClass = classOf[Container[Product1]],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List())
    )

    val node11 = Node(
      factoryClass = classOf[Container[Product1]],
      factoryUUID = "123",
      stage = 0,
      input = List(FactoryInput(runtime.universe.typeOf[String], classOf[External])),
      output = FactoryOutput(runtime.universe.typeOf[Int], List(classOf[Container[Product2]]))
    )

    val node2 = Node(
      factoryClass = classOf[Container[Product2]],
      factoryUUID = "123456",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product1]]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    val node3 = Node(
      factoryClass = classOf[Container[Product23]],
      factoryUUID = "1234567",
      stage = 1,
      input = List(
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product1]]),
        FactoryInput(runtime.universe.typeOf[Int], classOf[Container[Product2]])
      ),
      output = FactoryOutput(runtime.universe.typeOf[List[String]], List(classOf[Product23]))
    )

    assert(node1.targetNode(node2) === true)
    assert(node1.targetNode(node3) === true)

    assert(node11.targetNode(node2) === true)
    //    assert(node11.targetNode(node3) === false)
  }

}
