package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.internal.HasType

import scala.reflect.runtime

private[spark] case class FactoryInput(override val runtimeType: runtime.universe.Type,
                                       producer: Class[_]) extends HasType
