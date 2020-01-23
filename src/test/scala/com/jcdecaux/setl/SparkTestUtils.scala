package com.jcdecaux.setl

import org.apache.spark.SparkContext

object SparkTestUtils {

  def getActiveSparkContext: Option[SparkContext] = {
    val method = SparkContext.getClass.getDeclaredMethod("getActive")
    method.setAccessible(true)
    method.invoke(SparkContext).asInstanceOf[Option[SparkContext]]
  }

}
