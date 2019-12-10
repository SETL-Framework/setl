package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{Identifiable, Logging}

/**
 * A transformer can transform data into a type A
 *
 * @tparam T : Type of output data
 */
@InterfaceStability.Evolving
trait Transformer[T] extends Logging with Identifiable {

  /**
   * Get the transformed data
   *
   * @return
   */
  def transformed: T

  /**
   * Transform the current data
   */
  def transform(): this.type
}
