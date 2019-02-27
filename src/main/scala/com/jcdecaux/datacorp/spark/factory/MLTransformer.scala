package com.jcdecaux.datacorp.spark.factory

trait MLTransformer[T, M] extends Transformer[T] {
  var model: M = _

  /** Fit a model with the current data */
  def fit(): MLTransformer.this.type

  /** Load a model from a given path */
  def loadModel(path: String): MLTransformer.this.type

  /** Save the current model */
  def saveModel(path: String): MLTransformer.this.type
}
