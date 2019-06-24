package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.enums.ValueType

/**
  * Condition is used by [[com.jcdecaux.datacorp.spark.storage.v2.repository.Repository]] to find data
  *
  * @param key       key of the field
  * @param operator  e.g. ">", "<", ">=", "<=", "="
  * @param value     value to compare
  * @param valueType type of the value
  */
@InterfaceStability.Evolving
case class Condition(key: String, operator: String, value: Option[String], valueType: ValueType) {

}

object Condition {

  def apply(key: String, operator: String, value: String, valueType: ValueType): Condition = Condition(key, operator, Some(value), valueType)

  def apply(key: String, operator: String, value: String): Condition = Condition(key, operator, Some(value), ValueType.STRING)

  def apply(key: String, operator: String, value: Int): Condition = Condition(key, operator, Some(value.toString), ValueType.NUMBER)

  def apply(key: String, operator: String, value: Long): Condition = Condition(key, operator, Some(value.toString), ValueType.NUMBER)

  def apply(key: String, operator: String, value: Float): Condition = Condition(key, operator, Some(value.toString), ValueType.NUMBER)

  def apply(key: String, operator: String, value: Double): Condition = Condition(key, operator, Some(value.toString), ValueType.NUMBER)

  // TODO add constructors of timestamp and date

}