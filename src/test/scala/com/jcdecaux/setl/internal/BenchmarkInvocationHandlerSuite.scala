package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.Benchmark
import com.jcdecaux.setl.transformation.{AbstractFactory, Factory}
import com.jcdecaux.setl.workflow.Pipeline
import org.scalatest.funsuite.AnyFunSuite

class BenchmarkInvocationHandlerSuite extends AnyFunSuite {

  import BenchmarkInvocationHandlerSuite._

  test("BenchmarkInvocationHandler should log execution time") {
    val factory = new BenchmarkFactory

    val proxyFactory = java.lang.reflect.Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[AbstractFactory[_]]),
      new BenchmarkInvocationHandler(factory)
    ).asInstanceOf[AbstractFactory[_]]

    proxyFactory.read()
    proxyFactory.process()
    proxyFactory.write()

    assert(classOf[BenchmarkFactory].isAnnotationPresent(classOf[Benchmark]))
    assert(factory.get() === proxyFactory.get())

  }

  test("Benchmark should be handled in pipeline") {

    val pipeline = new Pipeline()

    pipeline
      .addStage[BenchmarkFactory]()
      .run()

  }

}

object BenchmarkInvocationHandlerSuite {

  @Benchmark
  class BenchmarkFactory extends Factory[String] {

    private[this] var data = ""

    override def read(): BenchmarkFactory.this.type = {
      data = "hahaha"
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
