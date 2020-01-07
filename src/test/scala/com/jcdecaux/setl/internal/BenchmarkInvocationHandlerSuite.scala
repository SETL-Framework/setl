package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.Benchmark
import com.jcdecaux.setl.transformation.{AbstractFactory, Factory}
import com.jcdecaux.setl.workflow.Pipeline
import org.scalatest.funsuite.AnyFunSuite

class BenchmarkInvocationHandlerSuite extends AnyFunSuite {

  import BenchmarkInvocationHandlerSuite._

  test("BenchmarkInvocationHandler should log execution time") {
    val factory = new BenchmarkFactory
    val benchmarkHandler = new BenchmarkInvocationHandler(factory)

    val proxyFactory = java.lang.reflect.Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[AbstractFactory[_]]),
      benchmarkHandler
    ).asInstanceOf[AbstractFactory[_]]

    proxyFactory.read()
    proxyFactory.process()
    proxyFactory.write()

    assert(classOf[BenchmarkFactory].isAnnotationPresent(classOf[Benchmark]))
    assert(factory.get() === proxyFactory.get())

    import scala.collection.JavaConverters._
    benchmarkHandler.getBenchmarkResult.asScala.foreach {
      x => assert(x._2 >=0)
    }

    assert(benchmarkHandler.getBenchmarkResult.size() === 2)

  }

  test("Benchmark should be handled in pipeline") {

    val pipeline = new Pipeline()

    val result = pipeline
      .addStage[BenchmarkFactory]()
      .benchmark(true)
      .run()
      .getBenchmarkResult

    assert(result.length === 1)

    val result2 = new Pipeline()
      .addStage[BenchmarkFactory]()
      .run()
      .getBenchmarkResult

    assert(result2.isEmpty)

    val result3 = new Pipeline()
      .addStage[BenchmarkFactory]()
      .benchmark(false)
      .run()
      .getBenchmarkResult

    assert(result3.isEmpty)
  }

}

object BenchmarkInvocationHandlerSuite {

  @Benchmark
  class BenchmarkFactory extends Factory[String] {

    private[this] var data = ""

    override def read(): BenchmarkFactory.this.type = {
      data = s"testing ${this.getClass.getSimpleName}... "
      this
    }

    @Benchmark
    override def process(): BenchmarkFactory.this.type = {
      data = data + data
      this
    }

    @Benchmark
    override def write(): BenchmarkFactory.this.type = {
      println(data)
      sleep()
      this
    }

    override def get(): String = data

    def sleep(): Unit = Thread.sleep(1000L)

  }

}
