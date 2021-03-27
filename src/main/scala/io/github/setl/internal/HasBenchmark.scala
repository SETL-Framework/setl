package io.github.setl.internal

import io.github.setl.BenchmarkResult

/**
 * HasBenchmark should be used for object having an aggregated benchmark. Typically a Pipeline or a Stage
 */
trait HasBenchmark {

  protected var _benchmark: Option[Boolean] = None

  /**
   * True if the benchmark will be measured, otherwise false
   *
   * @return boolean
   */
  def benchmark: Option[Boolean] = _benchmark

  /**
   * Set to true to enable the benchmarking
   *
   * @param boo true to enable benchmarking
   * @return this object
   */
  def benchmark(boo: Boolean): this.type = {
    _benchmark = Option(boo)
    this
  }

  /**
   * Get the aggregated benchmark result.
   *
   * @return an array of BenchmarkResult
   */
  def getBenchmarkResult: Array[BenchmarkResult]

}
