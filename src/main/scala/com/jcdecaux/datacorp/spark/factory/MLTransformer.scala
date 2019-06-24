package com.jcdecaux.datacorp.spark.factory

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

/**
  * A MLTransformer is a basic transformer with a ML model and ML-related functionality.
  *
  * @tparam T Data type of the transformer
  * @tparam M Type of the model
  */
@InterfaceStability.Evolving
trait MLTransformer[T, M] extends Transformer[T] {
  var model: M = _

  /** Fit a model with the current data */
  def fit(): MLTransformer.this.type

  /** Load a model from a given path */
  def loadModel(path: String): MLTransformer.this.type

  /** Save the current model */
  def saveModel(path: String): MLTransformer.this.type
}
