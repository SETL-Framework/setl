package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.ValueType


case class Condition(key: String, operator: String, value: Option[String], valueType: ValueType) {

}