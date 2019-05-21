package com.jcdecaux.datacorp.spark.factory

/**
  * A transformer can transform data into a type A
  *
  * @tparam T : Type of output data
  */
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
