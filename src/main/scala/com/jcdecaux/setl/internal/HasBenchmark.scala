package com.jcdecaux.setl.internal

import com.jcdecaux.setl.BenchmarkResult

trait HasBenchmark {

  protected var _benchmark: Option[Boolean] = None

  def benchmark: Option[Boolean] = _benchmark
  def benchmark(boo: Boolean): this.type = {
    _benchmark = Option(boo)
    this
  }

  def getBenchmarkResult: Array[BenchmarkResult]

}
