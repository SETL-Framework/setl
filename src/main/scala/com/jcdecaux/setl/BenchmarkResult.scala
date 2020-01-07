package com.jcdecaux.setl

case class BenchmarkResult(cls: String, read: Long, process: Long, write: Long, get: Long, total: Long) {

  override def toString: String = {

    val formatter = java.text.NumberFormat.getNumberInstance

    s"Benchmark class: $cls\n" +
    s"Total elapsed time: ${formatter.format(total)} ns\n" +
    s"read: ${formatter.format(read)} ns\n" +
    s"process: ${formatter.format(process)} ns\n" +
    s"write: ${formatter.format(write)} ns\n" +
    "================="
  }

}
