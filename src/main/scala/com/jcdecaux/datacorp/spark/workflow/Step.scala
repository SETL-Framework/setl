package com.jcdecaux.datacorp.spark.workflow

import org.apache.spark.sql.Dataset
import com.jcdecaux.datacorp.spark.factory.Factory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Step {

  private var previousStep: Option[Step] = None
  private var factories: ArrayBuffer[Factory[Dataset[_]]] = ArrayBuffer[Factory[Dataset[_]]]()

  def setPreviousStep(step: Step): Step = {
    previousStep = Some(step)
    this
  }

  def addFactory(factory: Factory[Dataset[_]]): Step = {
    factories += factory
    this
  }

  def run(): Step = {
    var inputs: mutable.Map[String, Dataset[_]] = mutable.HashMap.empty[String, Dataset[_]]
    var step: Option[Step] = previousStep

    while(step.isDefined) {
      inputs ++= step.get.factories.map(factory => (factory.getClass.getCanonicalName, factory.get())).toMap
      step = step.get.previousStep
    }

    println(inputs.size)

    inputs.foreach(println)

    factories.foreach(factory => {
      factory
        .setInputs(inputs.toMap)
        .read()
        .process()
        .write()
    })
    this
  }
}
