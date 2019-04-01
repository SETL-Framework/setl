package com.jcdecaux.datacorp.spark.util

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
    filters
      .filter(row => row.value.isDefined)
      .map(row => request(row))
      .mkString(" AND ")
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
    if(filter.value.isDefined) {
      filter.nature match {
        case "datetime" =>
          val time = if(filter.operator.contains(">")) "00:00:00" else "23:59:59"
          s"${filter.column} ${filter.operator} cast('${filter.value.get} $time' as timestamp)"
        case "date" =>
          s"${filter.column} ${filter.operator} cast('${filter.value.get}' as date)"
        case "string" =>
          s"${filter.column} ${filter.operator} '${filter.value.get}'"
        case _ =>
          s"${filter.column} ${filter.operator} ${filter.value.get}"
      }
    } else {
      ""
    }
  }
}
