package com.jcdecaux.datacorp.spark.internal

trait Configurable {

  def set(key: String, value: String): this.type

  def get(key: String): Option[String]

}
