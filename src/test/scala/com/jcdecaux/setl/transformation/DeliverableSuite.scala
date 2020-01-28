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

  test("Deliverable constructor") {
    val by: Byte = 0.toByte
    val s: Short = 1.toShort
    val i: Int = 2.toInt
    val l: Long = 3.toLong
    val f: Float = 4.toFloat
    val d: Double = 5.toDouble
    val bo: Boolean = true
    val c: Char = 6.toChar
    val str: String = "7"
    val test: Test = Test()
    val test2: Test2 = new Test2(1D, "test")
    val test3: Test3[Test2] = new Test3(test2)
    val test4: Test4[Test] = new Test4[Test](test)

    assert(new Deliverable[Byte](by).getPayload === by)
    assert(new Deliverable[Short](s).getPayload === s)
    assert(new Deliverable[Int](i).getPayload === i)
    assert(new Deliverable[Long](l).getPayload === l)
    assert(new Deliverable[Float](f).getPayload === f)
    assert(new Deliverable[Double](d).getPayload === d)
    assert(new Deliverable[Boolean](bo).getPayload === bo)
    assert(new Deliverable[Char](c).getPayload === c)
    assert(new Deliverable[String](str).getPayload === str)
    assert(new Deliverable[Test](test).getPayload === test)
    assert(new Deliverable[Test2](test2).getPayload.x === test2.x)
    assert(new Deliverable[Test2](test2).getPayload.y === test2.y)
    assert(new Deliverable[Test3[Test2]](test3).getPayload.x === test3.x)
    assert(new Deliverable[Test4[Test]](test4).getPayload.x === test4.x)
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

  case class Test(x: Int = 1)

  class Test2(val x: Double, val y: String) {

  }

  class Test3[T](val x: T) {

  }

  class Test4[T <: Test](val x: T) {

  }

}
