package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.ValueType

/**
  * Condition is used by [[Repository]] to find data
  *
  * @param key
  * @param operator
  * @param value
  * @param valueType
  */
case class Condition(key: String, operator: String, value: Option[String], valueType: ValueType) {

}