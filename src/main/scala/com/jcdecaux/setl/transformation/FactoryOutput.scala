package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.HasType

import scala.reflect.runtime

private[setl] case class FactoryOutput(override val runtimeType: runtime.universe.Type,
                                       consumer: Seq[Class[_ <: Factory[_]]],
                                       deliveryId: String = Deliverable.DEFAULT_ID) extends HasType
