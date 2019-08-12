package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey, Delivery}
import com.jcdecaux.datacorp.spark.transformation.Factory

object TestClasses {

  class Producer1

  class Producer2

  class TestFactory extends Factory[String] {

    var input3: Double = _
    var input4: Boolean = _

    @Delivery(producer = classOf[Producer1])
    var inputString1: String = _

    @Delivery(producer = classOf[Producer2])
    var inputString2: String = _

    @Delivery(optional = true)
    var inputInt: Int = _

    @Delivery
    def setInputs(d: Double, boo: Boolean): this.type = {
      input3 = d
      input4 = boo
      this
    }

    /**
      * Read data
      */
    override def read(): TestFactory.this.type = this

    /**
      * Process data
      */
    override def process(): TestFactory.this.type = this

    /**
      * Write data
      */
    override def write(): TestFactory.this.type = this

    /**
      * Get the processed data
      */
    override def get(): String = "Product of TestFactory " + inputString1 + inputString2
  }


  case class MyObject(@ColumnName("col1") column1: String, column2: String)

  case class TestCompoundKey(@CompoundKey("primary", "1") a: String, @CompoundKey("primary", "2") b: Int, @CompoundKey("sort", "1") c: String)


}
