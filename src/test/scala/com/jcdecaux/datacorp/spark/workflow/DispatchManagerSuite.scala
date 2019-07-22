package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class MyFactory extends Factory[Container[Product23]] {

  var input: Container2[Product] = _
  var output: Container[Product23] = _

  @Delivery
  def setOutput(v: Container[Product23]): this.type = {
    output = v
    this
  }

  @Delivery
  def setInput(v: Container2[Product]): this.type = {
    input = v
    this
  }

  override def read(): MyFactory.this.type = this

  override def process(): MyFactory.this.type = this

  override def write(): MyFactory.this.type = this

  override def get(): Container[Product23] = output
}

class MyFactory2 extends Factory[Dataset[Product23]] with Serializable {

  var input: Dataset[Product] = _
  var output: Dataset[Product23] = _

  @Delivery
  def setInput(v: Dataset[Product]): this.type = {
    input = v
    this
  }

  @Delivery
  def setOutput(v: Dataset[Product23]): this.type = {
    output = v
    this
  }

  override def read(): this.type = this

  override def process(): this.type = this

  override def write(): this.type = this

  override def get(): Dataset[Product23] = output
}

class DispatchManagerSuite extends FunSuite {

  test("Test delivery manager with container and product") {

    val CP = Container(Product("1"))
    val C2P = Container2(Product("1"))
    val CP2 = Container(Product23("1"))
    val C2P2 = Container2(Product23("1"))

    val myFactory = new MyFactory

    val deliveryManager = new DispatchManager

    deliveryManager.setDelivery(new Deliverable(CP))
    deliveryManager.setDelivery(new Deliverable(C2P))
    deliveryManager.setDelivery(new Deliverable(CP2))
    deliveryManager.setDelivery(new Deliverable(C2P2))

    deliveryManager.dispatch(myFactory)

    assert(myFactory.input == C2P)
    assert(myFactory.output == CP2)

  }

  test("Test with dataset") {
    val spark = new SparkSessionBuilder("dev").setEnv("local").getOrCreate()

    import spark.implicits._

    val dsP1 = Seq(
      Product("a"), Product("b")
    ).toDS()

    val dsP2 = Seq(
      Product23("a2"), Product23("b2")
    ).toDS()

    val dsC1 = Seq(
      Container("a2"), Container("b2")
    ).toDS()

    val myFactory2 = new MyFactory2
    val deliveryManager = new DispatchManager

    deliveryManager.setDelivery(new Deliverable(dsP1))
    deliveryManager.setDelivery(new Deliverable(dsP2))
    deliveryManager.setDelivery(new Deliverable(dsC1))

    deliveryManager.dispatch(myFactory2)

    assert(myFactory2.input.count() === 2)
    assert(myFactory2.input.collect().count(_.x === "a") === 1)
    assert(myFactory2.input.collect().count(_.x === "a2") === 0)
    assert(myFactory2.output.count() === 2)
    assert(myFactory2.output.collect().count(_.x === "a2") === 1)
    assert(myFactory2.output.collect().count(_.x === "b2") === 1)
    assert(myFactory2.output.collect().count(_.x === "a") === 0)

  }


  class P1

  class P2

  class Test extends Factory[String] {

    @Delivery(producer = classOf[P1], optional = true)
    var v1: String = _
    @Delivery(producer = classOf[P2], optional = true)
    var v2: String = _

    var output: String = _

    /**
      * Read data
      */
    override def read(): this.type = {
      if (v1 == null) v1 = "he"
      if (v2 == null) v2 = "ha"
      this
    }

    /**
      * Process data
      */
    override def process(): this.type = {
      output = v1 + v2
      this
    }

    /**
      * Write data
      */
    override def write(): this.type = this

    /**
      * Get the processed data
      */
    override def get(): String = output
  }

  test("Test optional input") {

    val test = new Test
    val dispatchManager = new DispatchManager

    dispatchManager.dispatch(test)

    assert(test.v1 === null)
    assert(test.v2 === null)

    dispatchManager.setDelivery(new Deliverable[String]("hehehe").setProducer(classOf[P1]))
    dispatchManager.setDelivery(new Deliverable[String]("hahaha").setProducer(classOf[P2]))
    dispatchManager.dispatch(test)

    assert(test.v1 === "hehehe")
    assert(test.v2 === "hahaha")
    assert(test.read().process().get() === "hehehehahaha")

  }
}
