package com.jcdecaux.datacorp.spark.factory

/**
  * A transformer can transform data into a type A
  *
  * @tparam A : Type of output data
  */
trait Transformer[A] {

  def transformed: A

  /** Process the current data */
  def transform(): this.type
}
