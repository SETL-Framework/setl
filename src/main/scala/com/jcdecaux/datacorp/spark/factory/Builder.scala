package com.jcdecaux.datacorp.spark.factory

/**
  * Builder could be used to build or initialize objects
  *
  * @tparam A the type of object that the builder is supposed to produce
  */
trait Builder[A] {

  /**
    * Build an object
    *
    * @return
    */
  def build(): this.type

  def get(): A
}
