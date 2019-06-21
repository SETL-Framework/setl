package com.jcdecaux.datacorp.spark.config

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.exception.SerializerException

import scala.reflect.runtime.universe._


class Conf extends Serializable {

  import Conf.Serializer

  private[config] val settings: ConcurrentHashMap[String, String] = new ConcurrentHashMap()

  def set(key: String, value: String): this.type = {
    settings.put(key, value)
    this
  }

  /**
    * Get a configuration under the string format
    *
    * @param key Key of the configuration
    * @return
    */
  def get(key: String): Option[String] = getAs[String](key)

  /**
    * Get a configuration and convert to the given type, return None if it's not set
    *
    * @param key       Key of the configuration
    * @param converter implicit converter
    * @tparam T define the type of output
    * @return
    */
  def getAs[T](key: String)(implicit converter: Serializer[T]): Option[T] = {
    getOption(key) match {
      case Some(thing) => converter.deserialize(thing)
      case _ => None
    }
  }

  def set[T](key: String, value: T)(implicit converter: Serializer[T]): this.type = {
    set(key, converter.serialize(value))
  }

  private[this] def getOption(key: String): Option[String] = Option(settings.get(key))

}

object Conf {

  trait Serializer[T] {

    @throws[SerializerException]
    def deserialize(v: String): Option[T]

    def serialize(v: T): String = v.toString

  }

  object Serializer {

    implicit val stringLoader: Serializer[String] = new Serializer[String] {
      override def deserialize(v: String): Option[String] = Some(v)
    }

    implicit val intLoader: Serializer[Int] = new Serializer[Int] {
      override def deserialize(v: String): Option[Int] = {
        val f = (v: String) => if (v != null) Some(v.toInt) else None
        deserializeTester(f, v)
      }
    }

    implicit val longLoader: Serializer[Long] = new Serializer[Long] {
      override def deserialize(v: String): Option[Long] = {
        val f = (v: String) => if (v != null) Some(v.toLong) else None
        deserializeTester(f, v)
      }
    }

    implicit val floatLoader: Serializer[Float] = new Serializer[Float] {
      override def deserialize(v: String): Option[Float] = {
        val f = (v: String) => if (v != null) Some(v.toFloat) else None
        deserializeTester(f, v)
      }
    }

    implicit val doubleLoader: Serializer[Double] = new Serializer[Double] {
      override def deserialize(v: String): Option[Double] = {
        val f = (v: String) => if (v != null) Some(v.toDouble) else None
        deserializeTester(f, v)
      }
    }

    implicit val booleanLoader: Serializer[Boolean] = new Serializer[Boolean] {
      override def deserialize(v: String): Option[Boolean] = {
        val f = (v: String) => if (v != null) Some(v.toBoolean) else None
        deserializeTester(f, v)
      }
    }

    implicit val iterableStringLoader: Serializer[Array[String]] = new Serializer[Array[String]] {
      override def deserialize(v: String): Option[Array[String]] =
        if (v != null) Some(v.split(",")) else None

      override def serialize(v: Array[String]): String = v.map(_.toString).mkString(",")
    }

    implicit val iterableIntLoader: Serializer[Array[Int]] = new Serializer[Array[Int]] {
      override def deserialize(v: String): Option[Array[Int]] = {
        val f = (v: String) => if (v != null) Some(v.split(",").map(_.toInt)) else None
        deserializeTester(f, v)
      }

      override def serialize(v: Array[Int]): String = v.map(_.toString).mkString(",")
    }

    implicit val iterableFloatLoader: Serializer[Array[Float]] = new Serializer[Array[Float]] {
      override def deserialize(v: String): Option[Array[Float]] = {
        val f = (v: String) => if (v != null) Some(v.split(",").map(_.toFloat)) else None
        deserializeTester(f, v)
      }

      override def serialize(v: Array[Float]): String = v.map(_.toString).mkString(",")
    }

    implicit val iterableDoubleLoader: Serializer[Array[Double]] = new Serializer[Array[Double]] {
      override def deserialize(v: String): Option[Array[Double]] = {
        val f = (v: String) => if (v != null) Some(v.split(",").map(_.toDouble)) else None
        deserializeTester(f, v)
      }

      override def serialize(v: Array[Double]): String = v.map(_.toString).mkString(",")
    }

    implicit val iterableLongLoader: Serializer[Array[Long]] = new Serializer[Array[Long]] {
      override def deserialize(v: String): Option[Array[Long]] = {
        val f = (v: String) => if (v != null) Some(v.split(",").map(_.toLong)) else None
        deserializeTester(f, v)
      }

      override def serialize(v: Array[Long]): String = v.map(_.toString).mkString(",")
    }

    implicit val iterableBooleanLoader: Serializer[Array[Boolean]] = new Serializer[Array[Boolean]] {
      override def deserialize(v: String): Option[Array[Boolean]] = {
        val f = (v: String) => if (v != null) Some(v.split(",").map(_.toBoolean)) else None
        deserializeTester(f, v)
      }

      override def serialize(v: Array[Boolean]): String = v.map(_.toString).mkString(",")
    }
  }

  private[this] def deserializeTester[T](f: String => Option[T], v: String)(implicit tag: TypeTag[T]): Option[T] = {
    val classOfT = tag.tpe match {
      case TypeRef(_, _, args) => args
    }

    try {
      f(v)
    } catch {
      case _: IllegalArgumentException => throw new SerializerException.Format(s"Can't convert $v to $classOfT")
      case _: NumberFormatException => throw new SerializerException.Format(s"Can't convert $v to $classOfT")
      case e: Throwable => throw e
    }
  }

}