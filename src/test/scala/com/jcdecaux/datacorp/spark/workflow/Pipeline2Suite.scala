package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.internal.Deliverable
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.scalatest.FunSuite

case class Product(x: String)

case class Product2(x: String, y: String)

case class Container[T](content: T)

case class Container2[T](content: T)

class ProductFactory extends Factory[Product] {

  var id: String = _

  @Delivery
  def setid(id: String): this.type = {
    this.id = id
    this
  }

  var output: Product = _

  override def read(): ProductFactory.this.type = this

  override def process(): ProductFactory.this.type = {
    output = Product(id)
    this
  }

  override def write(): ProductFactory.this.type = this

  override def get(): Product = output
}

class Product2Factory extends Factory[Product2] {

  var output: Product2 = _

  override def read(): this.type = this

  override def process(): this.type = {
    output = Product2("a", "b")
    this
  }

  override def write(): this.type = this

  override def get(): Product2 = output
}

class ContainerFactory extends Factory[Container[Product]] {

  var product1: Product = _
  var output: Container[Product] = _

  @Delivery
  def setProduct(v: Product): this.type = {
    this.product1 = v
    this
  }


  override def read(): ContainerFactory.this.type = this

  override def process(): ContainerFactory.this.type = {
    output = Container(product1)
    this
  }

  override def write(): ContainerFactory.this.type = this

  override def get(): Container[Product] = output
}

class Container2Factory extends Factory[Container2[Product2]] {

  var p2: Product2 = _
  var output: Container2[Product2] = _

  @Delivery
  def setProduct(v: Product2): this.type = {
    this.p2 = v
    this
  }


  override def read(): this.type = this

  override def process(): this.type = {
    output = Container2(p2)
    this
  }

  override def write(): this.type = this

  override def get(): Container2[Product2] = output
}

class Pipeline2Suite extends FunSuite {

  test("Test pipeline") {

    val f1 = new ProductFactory
    val f2 = new Product2Factory
    val f3 = new ContainerFactory
    val f4 = new Container2Factory

    val pipeline = new Pipeline2

    val stage1 = new Stage().addFactory(f1).addFactory(f2)
    val stage2 = new Stage().addFactory(f3)
    val stage3 = new Stage().addFactory(f4)

    pipeline
      .setInput(new Deliverable[String]("id_of_product1"))
      .addStage(stage1)
      .addStage(stage2)
      .addStage(stage3)
      .run()

    assert(pipeline.dispatchManagers.deliveries.length === 5)

  }


}
