package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

@InterfaceStability.Unstable
class Deliverable[T: Manifest](val value: T) {

  val canonicalName: Manifest[T] = manifest[T]

  def get: T = value

}
