package com.jcdecaux.setl.factory

import com.jcdecaux.setl.internal.TestClasses.TestFactory
import com.jcdecaux.setl.transformation.FactoryDeliveryMetadata
import com.jcdecaux.setl.workflow.External
import org.scalatest.funsuite.AnyFunSuite

class FactoryDeliveryMetadataSuite extends AnyFunSuite {

  val fac = new TestFactory

  test("Test FactoryDeliveryMetadata Builder") {

    val setters = FactoryDeliveryMetadata.builder().setFactory(fac).getOrCreate()

    setters.foreach(println)

    assert(setters.size === 4)
    assert(setters.map(_.factoryUUID).toSet.size === 1)
    assert(setters.find(_.name == "inputInt_$eq").get.producer === classOf[External])
    assert(setters.find(_.name == "setInputs").get.argTypes.size === 2)
  }
}
