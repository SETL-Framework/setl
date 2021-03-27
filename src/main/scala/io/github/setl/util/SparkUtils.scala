package io.github.setl.util

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.ExplainCommand

private[setl] object SparkUtils {

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

  def withSparkVersion[T](minVersion: String)(fun: Boolean => T): T = {
    try {
      fun(checkSparkVersion(minVersion))
    } catch {
      case e: NoSuchMethodException =>
        throw new NoSuchMethodException("Cannot instantiate ExplainCommand. " +
          s"Please check the implementation of its constructor in Spark $minVersion")
    }
  }

  def explainCommandWithExtendedMode(logicalPlan: LogicalPlan): ExplainCommand = {
    withSparkVersion("3.0.0") { newer =>
      if (newer) {
        val extendedMode = Class.forName("org.apache.spark.sql.execution.ExtendedMode$")
          .getField("MODULE$")
          .get(Class.forName("org.apache.spark.sql.execution.ExtendedMode$"))
          .asInstanceOf[Object]

        classOf[ExplainCommand]
          .getConstructor(classOf[LogicalPlan], Class.forName("org.apache.spark.sql.execution.ExplainMode"))
          .newInstance(logicalPlan, extendedMode)
      } else {
        classOf[ExplainCommand]
          .getConstructor(classOf[LogicalPlan], classOf[Boolean], classOf[Boolean], classOf[Boolean])
          .newInstance(logicalPlan, true.asInstanceOf[java.lang.Boolean], false.asInstanceOf[java.lang.Boolean], false.asInstanceOf[java.lang.Boolean])
      }
    }
  }
}
