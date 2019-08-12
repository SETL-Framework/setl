package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

import com.jcdecaux.datacorp.spark.exception.InvalidDeliveryException
import com.jcdecaux.datacorp.spark.internal.{Identifiable, Logging}
import com.jcdecaux.datacorp.spark.transformation.{FactoryInput, FactoryOutput}

import scala.reflect.runtime

/**
  * Node is a representation of Factory in the DAG. One node could have multiple inputs and one single output.
  *
  * @param factoryClass class of the represented Factory
  * @param factoryUUID  UUID of the represented Factory
  * @param stage        stage where the node is located in the DAG
  * @param input        input of node
  * @param output       output of node
  */
private[workflow] case class Node(factoryClass: Class[_],
                                  factoryUUID: UUID,
                                  stage: Int,
                                  input: List[FactoryInput],
                                  output: FactoryOutput) extends Identifiable with Logging {

  def listInputProducers: List[Class[_]] = this.input.map(_.producer)

  def findInputByType(t: runtime.universe.Type): List[FactoryInput] = input.filter(_.runtimeType == t)

  def describe(): Unit = {
    println(s"Node   : $getPrettyName")
    println(s"Stage  : $stage")
    input.foreach(i => println(s"Input  : ${cleanTypeName(i.runtimeType)}"))
    println(s"Output : ${cleanTypeName(output.runtimeType)}") //
    println("--------------------------------------")
  }

  private[workflow] def getPrettyName: String = prettyNameOf(factoryClass.getCanonicalName)

  private[workflow] def prettyNameOf(t: String): String = t.split("\\.").last

  private[workflow] def cleanTypeName(t: runtime.universe.Type): String = {
    t.toString.split("\\[").map(prettyNameOf).mkString("[")
  }

  /**
    * For a new Node, return true if it is a target node of the current node, false otherwise
    *
    * @param next another [[Node]]
    */
  def targetNode(next: Node): Boolean = {

    val validNodeUUID = if (this.getUUID == next.getUUID) {
      log.warn("The two nodes have the same UUID")
      false
    } else true

    val validClassUUID = if (this.factoryUUID == next.factoryUUID) {
      log.warn("The two nodes are representing one same factory")
      false
    } else true

    val validStage = if (this.stage >= next.stage) {
      log.warn("The two nodes are in the same stage")
      false
    } else true

    var validTarget: Boolean = false

    val filteredInputs = next.findInputByType(this.output.runtimeType)

    validTarget = filteredInputs.length match {
      case 0 => false
      case 1 => handleOneSingleMatchedInput(filteredInputs.head, next) // Found only one matching type
      case _ => handleMultipleMatchedInputs(filteredInputs, next) // Multiple variables of the same type were found in the next node
    }

    validStage && validTarget && validNodeUUID && validClassUUID
  }

  private[this] def handleOneSingleMatchedInput(matchedInput: FactoryInput, nextNode: Node): Boolean = {

    var validConsumer: Boolean = false
    var validProducer: Boolean = false

    // producer is valid if the input producer is not set or the input producer is the current node
    if (matchedInput.producer == classOf[External] || matchedInput.producer == this.factoryClass) {
      validProducer = true
    }

    if (this.output.consumer.isEmpty || this.output.consumer.contains(nextNode.factoryClass)) {
      validConsumer = true
    }

    validConsumer && validProducer
  }

  private[this] def handleMultipleMatchedInputs(matchedInputs: List[FactoryInput], nextNode: Node): Boolean = {

    var validConsumer: Boolean = false
    var validProducer: Boolean = false

    if (this.output.consumer.isEmpty || this.output.consumer.contains(nextNode.factoryClass)) {
      validConsumer = true
    }

    // if there exists some filteredInputs whose explicitly defined producer matches the class of the current node.
    val exactProducerMatch = matchedInputs.exists(_.producer == this.factoryClass)

    val nonExplicitlyDefinedProducers = matchedInputs.filter(_.producer == classOf[External])

    if (!exactProducerMatch && nonExplicitlyDefinedProducers.length > 1) {
      throw new InvalidDeliveryException(s"Multiple inputs in ${nextNode.getPrettyName} are " +
        s"of type ${this.output.runtimeType.toString}. You may have to explicitly declare their producer " +
        s"in the Delivery annotation, otherwise this may cause unexpected pipeline result.")
    }

    validProducer = exactProducerMatch || nonExplicitlyDefinedProducers.length == 1

    validConsumer && validProducer
  }


}
