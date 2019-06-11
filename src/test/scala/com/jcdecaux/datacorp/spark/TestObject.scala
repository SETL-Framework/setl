package com.jcdecaux.datacorp.spark

import java.sql.{Date, Timestamp}


case class TestObject(partition1: Int, partition2: String, clustering1: String, value: Long)

case class TestObject2(col1: String, col2: Int, col3: Double, col4: Timestamp, col5: Date, col6: Long)