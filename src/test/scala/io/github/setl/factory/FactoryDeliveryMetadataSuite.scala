package io.github.setl.factory

import io.github.setl.internal.TestClasses.TestFactory
import io.github.setl.transformation.FactoryDeliveryMetadata
import io.github.setl.workflow.External
import org.scalatest.funsuite.AnyFunSuite

class FactoryDeliveryMetadataSuite extends AnyFunSuite {

  val fac = new TestFactory

  test("Test FactoryDeliveryMetadata Builder") {

    val setters = FactoryDeliveryMetadata.builder().setFactory(fac).getOrCreate()

    setters.foreach(println)

    assert(setters.size === 4)
    assert(setters.map(_.factoryUUID).toSet.size === 1)
    assert(setters.find(_.name == "inputInt").get.producer === classOf[External])
    assert(setters.find(_.name == "setInputs").get.argTypes.size === 2)
    assert(setters.find(_.isDataset.contains(true)).size === 0)
  }
}
