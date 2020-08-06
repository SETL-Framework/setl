package com.jcdecaux.setl

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.SparkSession

private[setl] object SparkTestUtils {

  def getActiveSparkContext: Option[SparkContext] = {
    val method = SparkContext.getClass.getDeclaredMethod("getActive")
    method.setAccessible(true)
    method.invoke(SparkContext).asInstanceOf[Option[SparkContext]]
  }

  /**
   * Check if the current spark version is superior than the required version
   * @param requiredVersion minimum version of spark
   * @return true if the current spark is newer than the required version
   */
  def checkSparkVersion(requiredVersion: String): Boolean = {
    val currentVersion = SparkSession.getActiveSession match {
      case Some(ss) => ss.version
      case _ => throw new SparkException("No active Spark Session")
    }
    val targetVer = requiredVersion.replace(".", "") + "000"
    val thisVer = currentVersion.replace(".", "") + "000"
    thisVer.take(3).toInt >= targetVer.take(3).toInt
  }

}
