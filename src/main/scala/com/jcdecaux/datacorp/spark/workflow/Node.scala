package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.internal.{Identifiable, Logging}
import com.jcdecaux.datacorp.spark.transformation.{FactoryInput, FactoryOutput}

import scala.reflect.runtime

private[workflow] case class Node(classInfo: Class[_],
                                  classUUID: String,
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

  private[workflow] def getPrettyName: String = prettyNameOf(classInfo.getCanonicalName)

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

    val validUUID = if (this.getUUID == next.getUUID) false else true
    val validStage = if (this.stage >= next.stage) false else true
    var validTarget: Boolean = false

    val filteredInputs = next.findInputByType(this.output.runtimeType)

    filteredInputs.length match {

      case 0 =>
      case 1 => // Found only one matching type
        validTarget = true
      case _ => // Multiple variables of the same type were found in the next node:

        val validConsumer = if (this.output.consumer.nonEmpty && !this.output.consumer.contains(next.classInfo)) {
          false
        } else {
          true
        }

        val exactProducerMatch = filteredInputs.exists(_.producer == this.classInfo)
        val nonExplicitlyDefinedProducers = filteredInputs.filter(_.producer == classOf[Object])

        if (!exactProducerMatch && nonExplicitlyDefinedProducers.length > 1) {
          log.error(s"Multiple inputs in ${next.getPrettyName} are of type ${this.output.runtimeType.toString}. " +
            s"You may declare the producer information in Delivery annotation, otherwise this may cause " +
            s"unexpected pipeline result.")

          nonExplicitlyDefinedProducers.foreach { n => log.error(n) }
        }

        val validProducer = exactProducerMatch || nonExplicitlyDefinedProducers.length == 1

        validTarget = validConsumer && validProducer
    }

    validStage && validTarget && validUUID
  }
}
