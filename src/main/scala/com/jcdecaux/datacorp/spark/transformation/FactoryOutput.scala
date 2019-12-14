package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.internal.HasType

import scala.reflect.runtime

private[spark] case class FactoryOutput(override val runtimeType: runtime.universe.Type,
                                        consumer: Seq[Class[_ <: Factory[_]]],
                                        deliveryId: String = Deliverable.DEFAULT_ID) extends HasType
