package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.scalatest.FunSuite

class StageSuite extends FunSuite {

  import StageSuite._

  test("Stage Exceptions") {

    val stage = new Stage
    val fac = new MyFactoryStageTest

    assertThrows[AlreadyExistsException](stage.addFactory(fac).addFactory(fac))

  }

}

object StageSuite {

  class MyFactoryStageTest extends Factory[Container[Product23]] {

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

    override def read(): MyFactoryStageTest.this.type = this

    override def process(): MyFactoryStageTest.this.type = this

    override def write(): MyFactoryStageTest.this.type = this

    override def get(): Container[Product23] = output
  }
}
