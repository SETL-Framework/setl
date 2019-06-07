package com.jcdecaux.datacorp.spark.config

import java.util.concurrent.ConcurrentHashMap

import com.jcdecaux.datacorp.spark.util.TypeConverterImplicits

trait Conf extends Serializable with TypeConverterImplicits {

  private[config] val settings: ConcurrentHashMap[String, String] = new ConcurrentHashMap()

  def set(key: String, value: String): Conf = {
    settings.put(key, value)
    this
  }

  private[this] def getOption(key: String): Option[String] = Option(settings.get(key))

  /**
    * Get a configuration and convert to the given type, return None if it's not set
    *
    * @param key       Key of the configuration
    * @param converter implicit converter
    * @tparam T define the type of output
    * @return
    */
  def getAs[T](key: String)(implicit converter: TypeConverter[T]): Option[T] = {
    getOption(key) match {
      case Some(thing) => converter.convert(thing)
      case _ => None
    }
  }

  /**
    * Get a configuration under the string format
    *
    * @param key Key of the configuration
    * @return
    */
  def get(key: String): Option[String] = getAs[String](key)
}