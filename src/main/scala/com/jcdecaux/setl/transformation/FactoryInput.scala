package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.HasType

import scala.language.existentials
import scala.reflect.runtime

private[setl] case class FactoryInput(override val runtimeType: runtime.universe.Type,
                                      producer: Class[_],
                                      deliveryId: String = Deliverable.DEFAULT_ID) extends HasType
