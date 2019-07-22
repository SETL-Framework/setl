package com.jcdecaux.datacorp.spark.internal

import scala.reflect.runtime

case class FactoryInput(inputType: runtime.universe.Type, producer: Class[_]) {

}
