package com.jcdecaux.setl.storage.repository.streaming

import com.jcdecaux.setl.exception.InvalidConnectorException
import com.jcdecaux.setl.internal.CanWait
import com.jcdecaux.setl.storage.connector.{Connector, StreamingConnector}
import com.jcdecaux.setl.storage.repository.SparkRepository

import scala.reflect.runtime.universe.TypeTag

class StreamingRepository[T: TypeTag] extends SparkRepository[T] with CanWait {

  override def setConnector(connector: Connector): StreamingRepository.this.type = {
    connector match {
      case _: StreamingConnector => super.setConnector(connector)
      case _ => throw new InvalidConnectorException("The current connector is not a streaming connector")
    }
  }

  override def getConnector: StreamingConnector = {
    super.getConnector.asInstanceOf[StreamingConnector]
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   */
  override def awaitTermination(): Unit = this.getConnector.awaitTermination()

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @param timeout time to wait in milliseconds
   * @return `true` if it's stopped; or throw the reported error during the execution; or `false`
   *         if the waiting time elapsed before returning from the method.
   */
  override def awaitTerminationOrTimeout(timeout: Long): Unit = this.getConnector.awaitTerminationOrTimeout(timeout)

  /**
   * Stops the execution of this query if it is running.
   */
  override def stop(): Unit = this.getConnector.stop()

}

object StreamingRepository {

  def apply[T: TypeTag](sparkRepository: SparkRepository[T]): StreamingRepository[T] =
    new StreamingRepository[T]()
      .setConnector(sparkRepository.getConnector)
      .setReadCacheStorageLevel(sparkRepository.getReadCacheStorageLevel)
      .setUserDefinedSuffixKey(sparkRepository.getUserDefinedSuffixKey)
      .persistReadData(sparkRepository.persistReadData)

}
