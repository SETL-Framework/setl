package com.jcdecaux.datacorp.spark.exception

import com.jcdecaux.datacorp.spark.storage.Filter

/**
  * SqlExpressionUtil
  */
object SqlExpressionUtils {

  /**
    *
    * @param filters
    * @return
    */
  def build(filters: Set[Filter]): String = {
    filters.map{ filter =>
      request(filter)
    }.mkString(" AND ")
  }

  /**
    *
    * @param column
    * @param operator
    * @param nature
    * @param value
    * @return
    */
  def request(filter: Filter): String = {
    filter.nature match {
      case "datetime" =>
        val time = if(filter.operator.contains(">")) "00:00:00" else "23:59:59"
        s"${filter.column} ${filter.operator} cast('${filter.value} $time' as timestamp)"
      case "date" =>
        s"${filter.column} ${filter.operator} cast('${filter.value}' as date)"
      case "string" =>
        s"${filter.column} ${filter.operator} '${filter.value}'"
      case _ =>
        s"${filter.column} ${filter.operator} ${filter.value}"
    }
  }
}
