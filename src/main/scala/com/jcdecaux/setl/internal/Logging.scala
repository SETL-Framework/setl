package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability
import org.apache.log4j.{LogManager, Logger}

/**
 * Logging provide logging features for the class that extends this trait
 */
@InterfaceStability.Evolving
private[setl] trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var logger: Logger = _

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (logger == null) {
      logger = LogManager.getLogger(logName)
    }
    logger
  }

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String): Unit = log.warn(msg)

  protected def logError(msg: => String): Unit = log.error(msg)

}
