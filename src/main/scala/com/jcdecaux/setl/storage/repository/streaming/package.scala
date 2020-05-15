package com.jcdecaux.setl.storage.repository

import scala.reflect.runtime.universe.TypeTag

package object streaming {

  implicit class SparkRepositoryStreamingImplicits[T: TypeTag](sparkRepository: SparkRepository[T]) {

    def toStreamingRepository: StreamingRepository[T] = StreamingRepository(sparkRepository)

  }

}
