package io.github.setl

import io.github.setl.annotation.InterfaceStability
import io.github.setl.internal.Logging

/**
 * Builder could be used to build or initialize objects
 *
 * @tparam A the type of object that the builder is supposed to produce
 */
@InterfaceStability.Evolving
trait Builder[+A] extends Logging {

  /**
   * Build an object
   *
   * @return
   */
  def build(): this.type

  def get(): A

  def getOrCreate(): A = this.build().get()
}
