package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.internal.{Identifiable, Logging}

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
