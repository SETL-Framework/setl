package com.jcdecaux.datacorp.spark.storage

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
case class Filter(column: String, operator: String, nature: String, value: Option[String]) {
}
