package com.jcdecaux.datacorp.spark.internal


class Deliverable[T: Manifest](val value: T) {

  val canonicalName: Manifest[T] = manifest[T]

  def get: T = value

}
