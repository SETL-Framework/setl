package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.enums.Storage
import com.typesafe.config.{Config, ConfigException}

object ConfigUtils {

  private[spark] trait ConfigGetter[T] {
    def get(config: Config, path: String): Option[T]
  }

  private[spark] implicit val stringGetter: ConfigGetter[String] = new ConfigGetter[String] {
    override def get(config: Config, path: String): Option[String] = {
      try {
        Option(config.getString(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val intGetter: ConfigGetter[Int] = new ConfigGetter[Int] {
    override def get(config: Config, path: String): Option[Int] = {
      try {
        Option(config.getInt(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val longGetter: ConfigGetter[Long] = new ConfigGetter[Long] {
    override def get(config: Config, path: String): Option[Long] = {
      try {
        Option(config.getLong(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val floatGetter: ConfigGetter[Float] = new ConfigGetter[Float] {
    override def get(config: Config, path: String): Option[Float] = {
      try {
        Option(config.getLong(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val doubleGetter: ConfigGetter[Double] = new ConfigGetter[Double] {
    override def get(config: Config, path: String): Option[Double] = {
      try {
        Option(config.getLong(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val booleanGetter: ConfigGetter[Boolean] = new ConfigGetter[Boolean] {
    override def get(config: Config, path: String): Option[Boolean] = {
      try {
        Option(config.getBoolean(path))
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val listGetter: ConfigGetter[Array[AnyRef]] = new ConfigGetter[Array[AnyRef]] {
    override def get(config: Config, path: String): Option[Array[AnyRef]] = {
      try {
        Option(config.getList(path).unwrapped().toArray())
      } catch {
        case m: ConfigException.Missing => None
        case e: ConfigException.WrongType => throw e
      }
    }
  }

  private[spark] implicit val StorageGetter: ConfigGetter[Storage] = new ConfigGetter[Storage] {
    override def get(config: Config, path: String): Option[Storage] = {

      try {
        Option(Storage.valueOf(config.getString(path)))
      } catch {
        case m: ConfigException.Missing => throw m
        case w: ConfigException.WrongType => throw w
      }

    }
  }

  def getAs[T](config: Config, path: String)(implicit getter: ConfigGetter[T]): Option[T] = getter.get(config, path)

  def getList(config: Config, path: String): Option[Array[AnyRef]] = {
    try {
      Option(config.getList(path).unwrapped().toArray())
    } catch {
      case m: ConfigException.Missing => None
      case e: ConfigException.WrongType => throw e
    }
  }

  def isDefined(config: Config, path: String): Boolean = {
    try {
      config.getAnyRef(path) != null
    } catch {
      case _: ConfigException => false
    }
  }
}
