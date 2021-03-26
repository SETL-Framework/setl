package io.github.setl

case class BenchmarkResult(cls: String, read: Double, process: Double, write: Double, get: Double, total: Double) {

  override def toString: String = {

    val formatter = java.text.NumberFormat.getNumberInstance

    s"Benchmark class: $cls\n" +
      s"Total elapsed time: ${formatter.format(total)} s\n" +
      s"read: ${formatter.format(read)} s\n" +
      s"process: ${formatter.format(process)} s\n" +
      s"write: ${formatter.format(write)} s\n" +
      "================="
  }

}
