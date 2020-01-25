package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.workflow.External
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.{universe => ru}

class DeliverableSuite extends AnyFunSuite with Matchers {

  import com.jcdecaux.setl.transformation.DeliverableSuite._

  test("Empty Deliverable") {
    val del = new Deliverable[String](null)

    del.describe()
    assertThrows[NoSuchElementException](del.getPayload)
  }

  test("Deliverable consumer setting") {
    val del = new Deliverable[Float](0.1F)
    assert(del.consumer.isEmpty)
    del.setConsumer(classOf[Consumer1])
    assert(del.consumer.length === 1)
    assert(del.consumer.head === classOf[Consumer1])

    del.setConsumer(classOf[Consumer1])
    assert(del.consumer.length === 1)

    del.setConsumers(Array(classOf[Consumer2], classOf[Consumer3]))
    assert(del.consumer.length === 3)
    del.consumer should contain theSameElementsAs Array(classOf[Consumer1], classOf[Consumer2], classOf[Consumer3])
  }

  test("Delivery id") {
    val del = new Deliverable[Float](0.1F)
    assert(del.deliveryId.equals(Deliverable.DEFAULT_ID))

    del.setDeliveryId("testID")
    assert(del.deliveryId.equals("testID"))
  }

  test("Deliverable producer setting") {
    val del = new Deliverable[Float](0.1F)
    assert(del.producer === classOf[External])

    del.setProducer(classOf[Consumer3])
    assert(del.producer === classOf[Consumer3])

    assertDoesNotCompile("del.setProducer(classOf[String])")
  }

  test("Deliverable should compare their payload type") {
    val stringDelivery1 = new Deliverable[String]("str1")
    val stringDelivery2 = new Deliverable[String]("str2")
    val floatDelivery1 = new Deliverable[Float](1F)

    stringDelivery1.describe()
    assert(stringDelivery1.hasSamePayloadType(ru.typeOf[String]))
    assert(stringDelivery1.hasSamePayloadType(ru.typeOf[String].toString))
    assert(stringDelivery1.hasSamePayloadType(stringDelivery2))
    assert(!stringDelivery1.hasSamePayloadType(floatDelivery1))
  }

  test("Deliverable should compare their payload, producer and consumer") {
    val stringDelivery1 = new Deliverable[String]("str1")
    val stringDelivery2 = new Deliverable[String]("str2")
    val stringDelivery3 = new Deliverable[String]("str2").setConsumer(classOf[Consumer1])

    assert(stringDelivery1.sameDeliveryAs(stringDelivery2))
    assert(!stringDelivery2.sameDeliveryAs(stringDelivery3))
  }

  test("Deliverable null placeholder") {
    val empty = Deliverable.empty()

    assert(empty.isEmpty)
    assert(empty.getPayload === None)
  }

}

object DeliverableSuite {

  abstract class Consumer1 extends Factory[String]

  abstract class Consumer2 extends Factory[String]

  abstract class Consumer3 extends Factory[String]

}
