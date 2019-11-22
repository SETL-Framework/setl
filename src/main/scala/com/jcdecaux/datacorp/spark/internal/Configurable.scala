package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

@InterfaceStability.Evolving
trait Configurable {

  def set(key: String, value: String): this.type

  def get(key: String): Option[String]

}
