package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.ValueType

/**
  * Filter
  *
  * @param column   Column name
  * @param operator Equality and Relational Operators :
  *                 =     equal to
  *                 !=    not equal to
  *                 >     greater than
  *                 >=    greater than or equal to
  *                 <     less than
  *                 <=    less than or equal to
  * @param nature   datetime / date / string / long / int / double
  * @param value
  */
@deprecated("Filter will be replaced by condition", "v0.2.0")
case class Filter(column: String, operator: String, nature: String, value: Option[String]) {

  def toCondition: Condition = {
    Condition(
      key = this.column,
      operator = this.operator,
      value = this.value,
      valueType = this.nature.toLowerCase() match {
        case "string" => ValueType.STRING
        case "datetime" => ValueType.DATETIME
        case "date" => ValueType.DATE
        case "number" => ValueType.NUMBER
        case _ => ValueType.NUMBER
      }
    )
  }

}
