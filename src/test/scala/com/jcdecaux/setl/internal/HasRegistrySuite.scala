package com.jcdecaux.setl.internal

import com.jcdecaux.setl.exception.AlreadyExistsException
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class HasRegistrySuite extends AnyFunSuite with PrivateMethodTester {
  import com.jcdecaux.setl.internal.HasRegistrySuite._

  val item1: TestThing = TestThing("A")
  val item2: TestThing = TestThing("B")

  val registerNewItem = PrivateMethod[TestClass]('registerNewItem)
  val clearRegistry = PrivateMethod[TestClass]('clearRegistry)
  val registerNewItems = PrivateMethod[TestClass]('registerNewItems)

  test("HasRegistry should be able to add values") {
    val hasRegistry = new TestClass

    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.getRegisteredItem(item1.getUUID).get === item1)

    hasRegistry invokePrivate registerNewItem(item2)
    assert(hasRegistry.getRegisteredItem(item2.getUUID).get === item2)
  }

  test("HasRegistry should add multiple values") {
    val hasRegistry = new TestClass

    hasRegistry.invokePrivate(registerNewItems(Iterable(item1, item2)))
    assert(hasRegistry.getRegisteredItem(item1.getUUID).get === item1)
    assert(hasRegistry.getRegisteredItem(item2.getUUID).get === item2)
  }

  test("HasRegistry should throw exception when trying to add existing item") {
    val hasRegistry = new TestClass

    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.getRegisteredItem(item1.getUUID).get === item1)
    assertThrows[AlreadyExistsException](hasRegistry invokePrivate registerNewItem(item1))
  }

  test("HasRegistry should clear registry") {
    val hasRegistry = new TestClass

    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.getRegisteredItem(item1.getUUID).get === item1)
    hasRegistry invokePrivate clearRegistry()
    assert(hasRegistry.getRegisteredItem(item1.getUUID) === None)
    assert(hasRegistry.getRegistryLength === 0)
  }

  test("HasRegistry should be able to test if an item exists") {
    val hasRegistry = new TestClass

    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.hasRegisteredItem(item1.getUUID))
    assert(hasRegistry.hasRegisteredItem(item2.getUUID) === false)
  }

  test("HasRegistry should return registry length") {
    val hasRegistry = new TestClass

    assert(hasRegistry.getRegistryLength === 0)
    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.getRegistryLength === 1)
    hasRegistry invokePrivate registerNewItem(item2)
    assert(hasRegistry.getRegistryLength === 2)
  }

  test("HasRegistry should return is empty") {
    val hasRegistry = new TestClass

    assert(hasRegistry.isRegistryEmpty)
    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.isRegistryEmpty === false)
  }

  test("HasRegistry should return the last registered element") {
    val hasRegistry = new TestClass
    val item3 = TestThing("C")

    assert(hasRegistry.lastRegisteredItem === None)

    hasRegistry invokePrivate registerNewItem(item1)
    assert(hasRegistry.lastRegisteredItem === Option(item1))

    hasRegistry invokePrivate registerNewItem(item2)
    assert(hasRegistry.lastRegisteredItem === Option(item2))

    hasRegistry invokePrivate registerNewItem(item3)
    hasRegistry.lastRegisteredItem.get.testValue = "hehe"
    assert(hasRegistry.getRegisteredItem(item2.getUUID).get.testValue === "haha")
    assert(hasRegistry.lastRegisteredItem.get.testValue === "hehe")
  }

}

object HasRegistrySuite {

  case class TestThing(id: String) extends Identifiable {
    var testValue = "haha"
  }

  class TestClass extends HasRegistry[TestThing] {}

}
