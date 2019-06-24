package com.jcdecaux.datacorp.spark.factory

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

/**
  * Builder could be used to build or initialize objects
  *
  * @tparam A the type of object that the builder is supposed to produce
  */
@InterfaceStability.Evolving
trait Builder[A] {

  /**
    * Build an object
    *
    * @return
    */
  def build(): this.type

  def get(): A

  def getOrCreate(): A = this.build().get()
}
