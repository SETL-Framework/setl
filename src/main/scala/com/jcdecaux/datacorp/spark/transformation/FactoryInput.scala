package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.internal.HasType

import scala.language.existentials
import scala.reflect.runtime

private[spark] case class FactoryInput(override val runtimeType: runtime.universe.Type,
                                       producer: Class[_],
                                       deliveryId: String = Deliverable.DEFAULT_ID) extends HasType
