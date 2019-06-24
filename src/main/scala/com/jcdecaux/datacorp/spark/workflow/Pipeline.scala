package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.factory.Factory
import org.apache.spark.sql.Dataset

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  */
@InterfaceStability.Unstable
class Pipeline {

  val config: mutable.HashMap[String, String] = mutable.HashMap.empty[String, String]
  var steps: ArrayBuffer[Step] = ArrayBuffer[Step]()

  /**
    *
    * @param factory
    * @return
    */
  def addStep(factory: Factory[Dataset[_]]): Pipeline = {
    addStep(new Step().addFactory(factory))
    this
  }

  /**
    *
    * @param step
    * @return
    */
  def addStep(step: Step): Pipeline = {
    if (steps.nonEmpty)
      steps += step.setPreviousStep(steps.last)
    else
      steps += step
    this
  }

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def setConfig(key: String, value: String): Pipeline = {
    config += (key -> value)
    this
  }

  /**
    *
    * @return
    */
  def run(): Pipeline = {
    steps.foreach(step => {
      step.run()
    })
    this
  }
}
