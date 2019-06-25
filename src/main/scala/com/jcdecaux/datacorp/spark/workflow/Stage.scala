package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Deliverable
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Unstable
class Stage {

  val factories: ArrayBuffer[Factory[_]] = ArrayBuffer()
  var output: Array[Deliverable[_]] = _

  def addFactory(factory: Factory[_]): this.type = {
    factories += factory
    this
  }

  def run(): this.type = {
    output = factories
      .par
      .map(_.read().process().write().deliver())
      .toArray
    this
  }
}
