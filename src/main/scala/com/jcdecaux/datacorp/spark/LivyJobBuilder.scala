package com.jcdecaux.datacorp.spark

import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.internal.{Configurable, Logging}
import org.apache.livy.{Job, JobContext}

class LivyJobBuilder[T](val callback: Conf => T) extends Builder[Job[T]] with Configurable with Logging {

  def this(callback: java.util.function.Function[Conf, T]) = this(conf => callback(conf))

  private[this] var job: Job[T] = _

  private[this] val livyConfiguration: Conf = new Conf

  def set(key: String, value: String): this.type = {
    livyConfiguration.set(key, value)
    this
  }

  def set(options: Map[String, String]): this.type = {
    livyConfiguration.set(options)
    this
  }

  override def get(key: String): Option[String] = livyConfiguration.get(key)

  override def build(): LivyJobBuilder.this.type = {

    job = new Job[T] with Serializable {
      override def call(jobContext: JobContext): T = callback(livyConfiguration)
    }

    this
  }

  override def get(): Job[T] = job
}
