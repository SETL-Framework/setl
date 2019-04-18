package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.enums.ValueType


case class FilterCondition(key: String, operator: String, value: Option[String], valueType: ValueType) {

}