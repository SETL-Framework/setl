package com.jcdecaux.setl.storage.repository.streaming

import com.jcdecaux.setl.exception.InvalidConnectorException
import com.jcdecaux.setl.storage.connector.{Connector, StreamingConnector}
import com.jcdecaux.setl.storage.repository.SparkRepository

import scala.reflect.runtime.universe.TypeTag

class StreamingRepository[T: TypeTag] extends SparkRepository[T] {

  override def setConnector(connector: Connector): StreamingRepository.this.type = {
    connector match {
      case _: StreamingConnector => super.setConnector(connector)
      case _ => throw new InvalidConnectorException("The current connector is not a streaming connector")
    }
  }

  override def getConnector: StreamingConnector = {
    super.getConnector.asInstanceOf[StreamingConnector]
  }

}

object StreamingRepository {

  def apply[T: TypeTag](sparkRepository: SparkRepository[T]): StreamingRepository[T] =
    new StreamingRepository[T]()
      .setConnector(sparkRepository.getConnector)
      .setReadCacheStorageLevel(sparkRepository.getReadCacheStorageLevel)
      .setUserDefinedSuffixKey(sparkRepository.getUserDefinedSuffixKey)
      .persistReadData(sparkRepository.persistReadData)

}
