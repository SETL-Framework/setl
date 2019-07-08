package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging

/**
  * A transformer can transform data into a type A
  *
  * @tparam T : Type of output data
  */
@InterfaceStability.Evolving
trait Transformer[T] extends Logging {

  /**
    * Get the transformed data
    *
    * @return
    */
  def transformed: T

  /**
    * Transform the current data
    */
  def transform(): this.type
}
