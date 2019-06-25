package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

case class Product(x: String)

case class Product2(x: String)

case class Container[T](content: T)

case class Container2[T](content: T)

class MyFactory extends Factory[Container[Product2]] {

  var input: Container2[Product] = _
  var output: Container[Product2] = _

  @Delivery
  def setOutput(v: Container[Product2]): this.type = {
    output = v
    this
  }

  @Delivery
  def setInput(v: Container2[Product]): this.type = {
    input = v
    this
  }

  override def read(): Factory[Container[Product2]] = this

  override def process(): Factory[Container[Product2]] = this

  override def write(): Factory[Container[Product2]] = this

  override def get(): Container[Product2] = output
}

class MyFactory2 extends Factory[Dataset[Product2]] with Serializable {

  var input: Dataset[Product] = _
  var output: Dataset[Product2] = _

  @Delivery
  def setInput(v: Dataset[Product]): this.type = {
    input = v
    this
  }

  @Delivery
  def setOutput(v: Dataset[Product2]): this.type = {
    output = v
    this
  }

  override def read(): Factory[Dataset[Product2]] = this

  override def process(): Factory[Dataset[Product2]] = this

  override def write(): Factory[Dataset[Product2]] = this

  override def get(): Dataset[Product2] = output
}

class DispatchManagerSuite extends FunSuite {

  test("Test delivery manager with container and product") {

    val CP = Container(Product("1"))
    val C2P = Container2(Product("1"))
    val CP2 = Container(Product2("1"))
    val C2P2 = Container2(Product2("1"))

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
    val spark = new SparkSessionBuilder("dev").setEnv("dev").getOrCreate()

    import spark.implicits._

    val dsP1 = Seq(
      Product("a"), Product("b")
    ).toDS()

    val dsP2 = Seq(
      Product2("a2"), Product2("b2")
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
}
