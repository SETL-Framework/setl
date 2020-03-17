package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.HasType

import scala.language.existentials
import scala.reflect.runtime

/**
 * Metadata of an input of a Factory.
 *
 * If a `FactoryDeliveryMetadata` represents a method, then it may be converted to multiple FactoryInputs as each of its
 * arguments will be abstracted as a `FactoryInput`.
 *
 * @param runtimeType runtime type of the input
 * @param producer    producer of the input
 * @param deliveryId  delivery id of the input
 */
private[setl] case class FactoryInput(override val runtimeType: runtime.universe.Type,
                                      producer: Class[_],
                                      deliveryId: String = Deliverable.DEFAULT_ID,
                                      autoLoad: Boolean,
                                      optional: Boolean,
                                      factoryClass: Class[_ <: Factory[_]]) extends HasType
