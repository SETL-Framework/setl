package com.jcdecaux.setl.workflow

import scala.collection.mutable.ArrayBuffer

trait PipelineOptimizer {

  def setExecutionPlan(dag: DAG): this.type

  def optimize(stages: ArrayBuffer[Stage]): Array[Stage]

  def getOptimizedExecutionPlan: DAG

}
