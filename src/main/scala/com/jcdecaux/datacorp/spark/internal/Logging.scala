package com.jcdecaux.datacorp.spark.internal

import org.apache.log4j.{LogManager, Logger}


private[spark] trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var logger: Logger = _

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (logger == null) {
      logger = LogManager.getLogger(logName)
    }
    logger
  }

}
