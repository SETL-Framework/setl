package io.github.setl

import io.github.setl.util.SparkUtils
import org.apache.spark.SparkContext

private[setl] object SparkTestUtils {

  def getActiveSparkContext: Option[SparkContext] = {
    val method = SparkContext.getClass.getDeclaredMethod("getActive")
    method.setAccessible(true)
    method.invoke(SparkContext).asInstanceOf[Option[SparkContext]]
  }

  def checkSparkVersion(requiredVersion: String): Boolean = SparkUtils.checkSparkVersion(requiredVersion)

  def testConsolePrint(test: => Any, expected: String): Boolean = {
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream)(test)
    val result = stream.toString().trim()
    result == expected
  }

}
