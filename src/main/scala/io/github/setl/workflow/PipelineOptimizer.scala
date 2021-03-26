package io.github.setl.workflow

trait PipelineOptimizer {

  def setExecutionPlan(dag: DAG): this.type

  def optimize(stages: Iterable[Stage]): Array[Stage]

  def getOptimizedExecutionPlan: DAG

}
