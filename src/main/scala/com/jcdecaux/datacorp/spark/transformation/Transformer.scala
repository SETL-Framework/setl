package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

/**
  * A transformer can transform data into a type A
  *
  * @tparam T : Type of output data
  */
@InterfaceStability.Evolving
trait Transformer[T] {

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
