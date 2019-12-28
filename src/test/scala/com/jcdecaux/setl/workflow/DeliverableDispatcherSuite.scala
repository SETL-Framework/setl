package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.exception.{AlreadyExistsException, InvalidDeliveryException}
import com.jcdecaux.setl.storage.SparkRepositoryBuilder
import com.jcdecaux.setl.storage.connector.FileConnector
import com.jcdecaux.setl.transformation.{Deliverable, Factory}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DeliverableDispatcherSuite extends AnyFunSuite {

  import DeliverableDispatcherSuite._

  test("Test delivery manager with container and product") {

    val CP = Container(Product("1"))
    val C2P = Container2(Product("1"))
    val CP2 = Container(Product23("1"))
    val C2P2 = Container2(Product23("1"))

    val myFactory = new MyFactory

    val deliveryManager = new DeliverableDispatcher

    deliveryManager.addDeliverable(new Deliverable(CP))
    deliveryManager.addDeliverable(new Deliverable(C2P))
    deliveryManager.addDeliverable(new Deliverable(CP2))
    deliveryManager.addDeliverable(new Deliverable(C2P2))

    deliveryManager.testDispatch(myFactory)

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
    val deliveryManager = new DeliverableDispatcher

    deliveryManager.addDeliverable(new Deliverable(dsP1))
    deliveryManager.addDeliverable(new Deliverable(dsP2))
    deliveryManager.addDeliverable(new Deliverable(dsC1))

    deliveryManager.testDispatch(myFactory2)

    assert(myFactory2.input.count() === 2)
    assert(myFactory2.input.collect().count(_.x === "a") === 1)
    assert(myFactory2.input.collect().count(_.x === "a2") === 0)
    assert(myFactory2.output.count() === 2)
    assert(myFactory2.output.collect().count(_.x === "a2") === 1)
    assert(myFactory2.output.collect().count(_.x === "b2") === 1)
    assert(myFactory2.output.collect().count(_.x === "a") === 0)

  }


  abstract class P1 extends Factory[External]

  abstract class P2 extends Factory[External]

  class Test extends Factory[String] {

    @Delivery(producer = classOf[P1], optional = true)
    private[workflow] var v1: String = _
    @Delivery(producer = classOf[P2], optional = true)
    private[workflow] var v2: String = _

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

  test("Test DispatchManager optional input") {

    val test = new Test
    val dispatchManager = new DeliverableDispatcher

    dispatchManager.testDispatch(test)

    assert(test.v1 === null)
    assert(test.v2 === null)

    dispatchManager.addDeliverable(new Deliverable[String]("hehehe").setProducer(classOf[P1]))
    dispatchManager.addDeliverable(new Deliverable[String]("hahaha").setProducer(classOf[P2]))
    dispatchManager.testDispatch(test)

    assert(test.v1 === "hehehe")
    assert(test.v2 === "hahaha")
    assert(test.read().process().get() === "hehehehahaha")

  }

  test("DispatchManager should throw AlreadyExistsException exception " +
    "while trying setting two identical deliveries") {

    val dispatchManager = new DeliverableDispatcher
    val del = new Deliverable[String]("hehehe")
    assertThrows[AlreadyExistsException](dispatchManager.addDeliverable(del).addDeliverable(del))

  }

  class Factory1 extends Factory[String] {
    override def read(): Factory1.this.type = this

    override def process(): Factory1.this.type = this

    override def write(): Factory1.this.type = this

    override def get(): String = "factory 1"
  }

  class Factory2 extends Factory[String] {

    @Delivery(producer = classOf[Factory1])
    var input1: String = _
    @Delivery(producer = classOf[Factory1])
    var input2: String = _

    override def read(): Factory2.this.type = this

    override def process(): Factory2.this.type = this

    override def write(): Factory2.this.type = this

    override def get(): String = "factory 2"
  }

  test("DispatchManager should throw InvalidDeliveryException when there are multiple " +
    "matched deliveries") {

    val dispatchManager = new DeliverableDispatcher
    val del1 = new Deliverable[String]("hehe1").setProducer(classOf[Factory1]).setConsumer(classOf[Factory2])
    val del2 = new Deliverable[String]("hehe2").setProducer(classOf[Factory1]).setConsumer(classOf[Factory2])

    dispatchManager
      .addDeliverable(del1)
      .addDeliverable(del2)

    val factory2 = new Factory2

    assertThrows[InvalidDeliveryException](dispatchManager.testDispatch(factory2))
  }

  test("Deliverabl dispatcher should handle auto loading") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val repo = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_auto_load")
      .getOrCreate()

    val ds = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDS

    repo.save(ds)

    val dispatchManager = new DeliverableDispatcher
    dispatchManager.addDeliverable(new Deliverable(repo))
    val factoryWithAutoLoad = new FactoryWithAutoLoad
    dispatchManager.testDispatch(factoryWithAutoLoad)
    assert(factoryWithAutoLoad.input.count() === 3)
    repo.getConnector.asInstanceOf[FileConnector].delete()
  }

  test("Deliverabl dispatcher should handle auto loading with condition when no DS is available") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val repo = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_auto_load")
      .getOrCreate()

    val ds = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDS

    repo.save(ds)

    val dispatchManager = new DeliverableDispatcher
    dispatchManager.addDeliverable(new Deliverable(repo))
    val factoryWithAutoLoad = new FactoryWithAutoLoadWithCondition
    dispatchManager.testDispatch(factoryWithAutoLoad)
    factoryWithAutoLoad.input.show()
    assert(factoryWithAutoLoad.input.count() === 1)
    repo.getConnector.asInstanceOf[FileConnector].delete()
  }

  test("Deliverabl dispatcher should handle auto loading with condition when a DS is available") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDS

    val dispatchManager = new DeliverableDispatcher
    dispatchManager.addDeliverable(new Deliverable(ds))
    val factoryWithAutoLoad = new FactoryWithAutoLoadWithCondition
    dispatchManager.testDispatch(factoryWithAutoLoad)
    factoryWithAutoLoad.input.show()
    assert(factoryWithAutoLoad.input.count() === 1)
    assert(factoryWithAutoLoad.get().count() === 1)
    assert(factoryWithAutoLoad.get().getClass.isAssignableFrom(classOf[Dataset[Product2]]))
  }

  test("Deliverabl dispatcher should handle auto loading with condition when a data frame is available") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDF

    val dispatchManager = new DeliverableDispatcher
    val deliverable = new Deliverable(ds)

    println(deliverable.payloadType)
    dispatchManager.addDeliverable(deliverable)
    val factoryWithAutoLoad = new FactoryWithAutoLoadWithConditionDF
    dispatchManager.testDispatch(factoryWithAutoLoad)
    factoryWithAutoLoad.input.show()
    assert(factoryWithAutoLoad.input.count() === 1)
    assert(factoryWithAutoLoad.get().count() === 1)
    assert(factoryWithAutoLoad.get().getClass.isAssignableFrom(classOf[Dataset[Product2]]))
    assert(factoryWithAutoLoad.get().getClass.isAssignableFrom(classOf[DataFrame]))
  }

  test("Deliverable dispatcher should throw exception when repository for auto loading has a wrong consumer") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val repo = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_auto_load")
      .getOrCreate()

    val dispatchManager = new DeliverableDispatcher
    dispatchManager.addDeliverable(new Deliverable(repo).setConsumer(classOf[MyFactory2]))
    val factoryWithAutoLoad = new FactoryWithAutoLoad
    assertThrows[InvalidDeliveryException](dispatchManager.testDispatch(factoryWithAutoLoad))
  }

  test("Deliverable dispatcher should throw exception when there is neither delivery nor repository") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val repo = new SparkRepositoryBuilder[Product2](Storage.CSV)
      .setPath("src/test/resources/csv_test_auto_load_optional")
      .getOrCreate()

    val dispatchManager = new DeliverableDispatcher
    dispatchManager.addDeliverable(new Deliverable(repo))

    val factoryWithAutoLoadException = new FactoryWithAutoLoadException
    val factoryWithOptional = new FactoryWithAutoLoadOptional

    val ds = Seq(
      Product2("a", "1"),
      Product2("b", "2"),
      Product2("c", "3")
    ).toDS

    repo.save(ds)

    dispatchManager.testDispatch(factoryWithOptional)
    assert(factoryWithOptional.input.count() === 3)
    assert(factoryWithOptional.product23 === null)

    assertThrows[NoSuchElementException](dispatchManager.testDispatch(factoryWithAutoLoadException))
    repo.getConnector.asInstanceOf[FileConnector].delete()
  }

  test("Deliverable dispatcher should handle delivery ID when there are multiple deliveries of the same type") {

    val arrayOne = new Deliverable[Array[String]](Array("a", "b")).setDeliveryId("first")
    val arrayTwo = new Deliverable[Array[String]](Array("1", "2")).setDeliveryId("second")
    val arrayThree = new Deliverable[Array[String]](Array("x", "y"))
    val arrayFour = new Deliverable[Array[String]](Array("m", "n")).setDeliveryId("fourth")

    val dispatcher = new DeliverableDispatcher
    dispatcher
      .addDeliverable(arrayOne)
      .addDeliverable(arrayTwo)
      .addDeliverable(arrayThree)
      .addDeliverable(arrayFour)

    val multipleInputFactory = new MultipleInputFactory
    dispatcher.testDispatch(multipleInputFactory)
    val output = multipleInputFactory.read().process().write().get()
    assert(output === Array("a", "b", "1", "2", "x", "y"))
  }

  test("Deliverable dispatcher should throw exception when ID not matching") {

    val arrayOne = new Deliverable[Array[String]](Array("a", "b")).setDeliveryId("first")
    val arrayTwo = new Deliverable[Array[String]](Array("1", "2")).setDeliveryId("second")
    val arrayThree = new Deliverable[Array[String]](Array("x", "y"))
    val arrayFour = new Deliverable[Array[String]](Array("m", "n"))

    val dispatcher = new DeliverableDispatcher
    dispatcher
      .addDeliverable(arrayOne)
      .addDeliverable(arrayTwo)
      .addDeliverable(arrayThree)
      .addDeliverable(arrayFour)

    val multipleInputFactory = new MultipleInputFactory
    assertThrows[NoSuchElementException](dispatcher.testDispatch(multipleInputFactory))
  }
}

object DeliverableDispatcherSuite {

  class MultipleInputFactory extends Factory[Array[String]] {

    @Delivery(id = "first")
    var arrayOne: Array[String] = _

    @Delivery(id = "second")
    var arrayTwo: Array[String] = _

    @Delivery
    var arrayThree: Array[String] = _

    /**
     * Read data
     */
    override def read(): MultipleInputFactory.this.type = this

    /**
     * Process data
     */
    override def process(): MultipleInputFactory.this.type = this

    /**
     * Write data
     */
    override def write(): MultipleInputFactory.this.type = this

    /**
     * Get the processed data
     */
    override def get(): Array[String] = arrayOne ++ arrayTwo ++ arrayThree
  }

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

  class FactoryWithAutoLoad extends Factory[Dataset[Product2]] {
    @Delivery(autoLoad = true)
    var input: Dataset[Product2] = _

    override def read(): FactoryWithAutoLoad.this.type = this

    override def process(): FactoryWithAutoLoad.this.type = this

    override def write(): FactoryWithAutoLoad.this.type = this

    override def get(): Dataset[Product2] = input
  }

  class FactoryWithAutoLoadWithCondition extends Factory[Dataset[Product2]] {
    @Delivery(autoLoad = true, condition = "x = 'a'")
    var input: Dataset[Product2] = _

    override def read(): FactoryWithAutoLoadWithCondition.this.type = this

    override def process(): FactoryWithAutoLoadWithCondition.this.type = this

    override def write(): FactoryWithAutoLoadWithCondition.this.type = {

      println(input.getClass)

      this
    }

    override def get(): Dataset[Product2] = input
  }

  class FactoryWithAutoLoadWithConditionDF extends Factory[DataFrame] {
    @Delivery(autoLoad = true, condition = "x = 'a'")
    var input: DataFrame = _

    override def read(): FactoryWithAutoLoadWithConditionDF.this.type = this

    override def process(): FactoryWithAutoLoadWithConditionDF.this.type = this

    override def write(): FactoryWithAutoLoadWithConditionDF.this.type = {

      println(input.getClass)

      this
    }

    override def get(): DataFrame = input
  }

  class FactoryWithAutoLoadException extends Factory[Dataset[Product2]] {
    @Delivery(autoLoad = true)
    var input: Dataset[Product2] = _

    @Delivery(autoLoad = true)
    var product23: Dataset[Product23] = _

    override def read(): FactoryWithAutoLoadException.this.type = this

    override def process(): FactoryWithAutoLoadException.this.type = this

    override def write(): FactoryWithAutoLoadException.this.type = this

    override def get(): Dataset[Product2] = input
  }

  class FactoryWithAutoLoadOptional extends Factory[Dataset[Product2]] {
    @Delivery(autoLoad = true)
    var input: Dataset[Product2] = _

    @Delivery(autoLoad = true, optional = true)
    var product23: Dataset[Product23] = _

    override def read(): FactoryWithAutoLoadOptional.this.type = this

    override def process(): FactoryWithAutoLoadOptional.this.type = this

    override def write(): FactoryWithAutoLoadOptional.this.type = this

    override def get(): Dataset[Product2] = input
  }

}