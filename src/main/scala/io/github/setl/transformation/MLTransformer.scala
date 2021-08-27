package io.github.setl.transformation
import org.apache.spark.ml.{Transformer => SparkTransformer}
import io.github.setl.annotation.InterfaceStability
import org.apache.hadoop.fs.Path

/**
 * A MLTransformer is a basic transformer with a ML model and ML-related functionality.
 *
 * @tparam T Data type of the transformer
 */
@InterfaceStability.Evolving
trait MLTransformer[T, M <: SparkTransformer] extends Transformer[T] {

  var model: M = _
  val modelPath: Path
  var overwriteModel: Boolean = false

  /** Fit a model with the current data */
  def fit(): MLTransformer.this.type

  /** Load a model from a given path */
  def loadModel(): MLTransformer.this.type

  /** Save the current model */
  def saveModel(): MLTransformer.this.type
}
