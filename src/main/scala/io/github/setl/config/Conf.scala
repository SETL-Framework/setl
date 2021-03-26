package io.github.setl.config

import java.util.concurrent.ConcurrentHashMap

import io.github.setl.annotation.InterfaceStability
import io.github.setl.enums.Storage
import io.github.setl.exception.ConfException
import io.github.setl.internal.Configurable
import io.github.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config

import scala.reflect.runtime.universe._

/**
 * Conf is a class that holds data of configuration.
 * Internally, Conf use a [[ConcurrentHashMap]] for saving the configuration arguments.
 *
 * We can instantiate a Conf directly from a Scala [[Map]] or a [[Config]] from the typesafe config library.
 *
 * {{{
 *   val conf1 = Conf(myMap)  // equivalent to Conf.fromMap(myMap)
 *   val conf2 = Conf(myConfig)  //equivalent to Conf.fromConfig(myConfig)
 * }}}
 *
 * We can also instantiate manually
 *
 * {{{
 *   val conf3 = new Conf()
 *   conf3.set("k", "v")
 *   conf3.set(Map("k1" -> "v1", "k2" -> "v2"))
 *   conf3 += anotherConf
 * }}}
 *
 * Configuration arguments can be retrieved by calling the get() method. Conf provide also several implicit converters that
 * handle the type conversion.
 *
 * For example:
 * {{{
 *   val double = conf.getAs[Double]("dKey")
 *   val intArray = conf.getAs[Array[Int]]("aiKey")
 * }}}
 */
@InterfaceStability.Evolving
class Conf extends Serializable with Configurable {

  import Conf.Serializer

  private[config] val settings: ConcurrentHashMap[String, String] = new ConcurrentHashMap()

  def set(key: String, value: String): this.type = {
    settings.put(key, value)
    this
  }

  def set(options: Map[String, String]): this.type = {
    options.foreach(x => this.set(x._1, x._2))
    this
  }

  def +=(conf: Conf): this.type = {
    settings.putAll(conf.settings)
    this
  }

  def has(key: String): Boolean = if (settings.containsKey(key)) true else false

  /**
   * Get a configuration under the string format
   *
   * @param key Key of the configuration
   * @return
   */
  def get(key: String): Option[String] = getAs[String](key)

  /**
   * Get a configuration under the string format
   *
   * @param key Key of the configuration
   * @return
   */
  def get(key: String, defaultValue: String): String = getAs[String](key).getOrElse(defaultValue)

  /**
   * Get a configuration and convert to the given type, return None if it's not set
   *
   * @param key       Key of the configuration
   * @param converter implicit converter
   * @tparam T define the type of output
   * @return
   */
  @throws[ConfException]
  def getAs[T](key: String)(implicit converter: Serializer[T]): Option[T] = {
    getOption(key) match {
      case Some(thing) => converter.deserialize(thing)
      case _ => None
    }
  }

  /**
   * Get a configuration and convert to the given type, return a default value if it's not set
   *
   * @param key Key of the configuration
   * @tparam T define the type of output
   * @return
   */
  def getAs[T: Serializer](key: String, defaultValue: T): T = this.getAs[T](key).getOrElse(defaultValue)

  def set[T](key: String, value: T)(implicit converter: Serializer[T]): this.type = {
    set(key, converter.serialize(value))
  }

  private[this] def getOption(key: String): Option[String] = Option(settings.get(key))

  def toMap: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap
  }

}

object Conf {

  def apply(options: Map[String, String]): Conf = fromMap(options)

  def apply(options: Config): Conf = fromConfig(options)

  def fromMap(options: Map[String, String]): Conf = new Conf().set(options)

  def fromConfig(options: Config): Conf = fromMap(TypesafeConfigUtils.getMap(options))

  trait Serializer[T] {

    @throws[ConfException]
    def deserialize(v: String): Option[T]

    def serialize(v: T): String = v.toString

  }

  object Serializer {

    implicit val storageLoader: Serializer[Storage] = new Serializer[Storage] {
      override def deserialize(v: String): Option[Storage] = {
        val f = (v: String) => Some(Storage.valueOf(v))
        deserializeTester(f, v)
      }
    }

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
      case _: NumberFormatException => throw new ConfException.Format(s"Can't convert $v to $classOfT")
      case _: IllegalArgumentException => throw new ConfException.Format(s"Can't convert $v to $classOfT")
      case e: Throwable => throw e
    }
  }

}
