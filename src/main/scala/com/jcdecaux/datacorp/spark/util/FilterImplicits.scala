package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.enums.ValueType
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.storage.Condition

object FilterImplicits extends Logging {


  implicit class ConditionsToRequest(conditions: Set[Condition]) {

    /**
      * Convert a [[Set]] of [[com.jcdecaux.datacorp.spark.storage.Condition]] objects to a spark SQL query string
      *
      * @throws IllegalArgumentException if a datetime/date filter doesn't have a value with correct format,
      *                                  an illegal argument exception will be thrown
      * @return String
      */
    @throws[IllegalArgumentException]
    def toSqlRequest: String = {
      val query = conditions
        .filter(row => row.value.isDefined)
        .map(_.toSqlRequest)
        .mkString(" AND ")

      log.debug(s"Query: $query")
      query
    }
  }

  implicit class ConditionToRequest(condition: Condition) {

    /**
      * Convert a [[com.jcdecaux.datacorp.spark.storage.Condition]] object to a spark SQL query string
      *
      * @throws IllegalArgumentException if a datetime/date filter doesn't have a value with correct format,
      *                                  an illegal argument exception will be thrown
      * @return String
      */
    @throws[IllegalArgumentException]
    def toSqlRequest: String = {
      val query: String = if (condition.value.isDefined) {
        condition.valueType match {
          case ValueType.DATETIME =>
            val t = DateUtils.reformatDateTimeString(condition.value.get, withTime = true, end = if (condition.operator.contains(">")) false else true)
            s"${condition.key} ${condition.operator} cast('$t' as ${condition.valueType.value})"
          case ValueType.DATE =>
            val t = DateUtils.reformatDateTimeString(condition.value.get, withTime = false, end = if (condition.operator.contains(">")) false else true)
            s"${condition.key} ${condition.operator} cast('$t' as ${condition.valueType.value})"
          case ValueType.STRING =>
            s"${condition.key} ${condition.operator} '${condition.value.get}'"
          case ValueType.NUMBER =>
            s"${condition.key} ${condition.operator} ${condition.value.get}"
        }
      } else {
        ""
      }

      log.debug(s"Query: $query")
      query
    }
  }

}
