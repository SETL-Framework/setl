package com.jcdecaux.datacorp.spark.storage

/**
  *
  * @param column   Column name
  * @param operator Equality and Relational Operators :
  *                 =     equal to
  *                 =     equal to
  *                 !=    not equal to
  *                 >     greater than
  *                 >=    greater than or equal to
  *                 <     less than
  *                 <=    less than or equal to
  * @param nature
  * @param value
  */
case class Filter(column: String, operator: String, nature: String, value: String)
