package com.jcdecaux.datacorp.spark

package object workflow {

  case class Product1(x: String)

  case class Product2(x: String, y: String)

  case class Product(x: String)

  case class Product23(x: String)

  case class Container[T](content: T)

  case class Container2[T](content: T)

}
