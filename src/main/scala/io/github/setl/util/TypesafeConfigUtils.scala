package io.github.setl.util

import io.github.setl.enums.Storage
import com.typesafe.config.{Config, ConfigException}

object TypesafeConfigUtils {

  @throws[com.typesafe.config.ConfigException]
  def getAs[T](config: Config, path: String)(implicit getter: ConfigGetter[T]): Option[T] = getter.get(config, path)

  private[this] def _get[T](path: String): (String => T) => Option[T] = (fun: String => T) => {
    try {
      Option(fun(path))
    } catch {
      case _: ConfigException.Missing => None
      case e: ConfigException.WrongType => throw e
    }
  }

  private[setl] implicit val stringGetter: ConfigGetter[String] = new ConfigGetter[String] {
    override def get(config: Config, path: String): Option[String] = {
      _get[String](path)(config.getString)
    }
  }

  private[setl] implicit val intGetter: ConfigGetter[Int] = new ConfigGetter[Int] {
    override def get(config: Config, path: String): Option[Int] = {
      _get[Int](path)(config.getInt)
    }
  }

  private[setl] implicit val longGetter: ConfigGetter[Long] = new ConfigGetter[Long] {
    override def get(config: Config, path: String): Option[Long] = {
      _get[Long](path)(config.getLong)
    }
  }

  private[setl] implicit val floatGetter: ConfigGetter[Float] = new ConfigGetter[Float] {
    override def get(config: Config, path: String): Option[Float] = {
      _get[Float](path)(x => config.getString(x).toFloat)
    }
  }

  private[setl] implicit val doubleGetter: ConfigGetter[Double] = new ConfigGetter[Double] {
    override def get(config: Config, path: String): Option[Double] = {
      _get[Double](path)(config.getDouble)
    }
  }

  private[setl] implicit val booleanGetter: ConfigGetter[Boolean] = new ConfigGetter[Boolean] {
    override def get(config: Config, path: String): Option[Boolean] = {
      _get[Boolean](path)(config.getBoolean)
    }
  }

  private[setl] implicit val listGetter: ConfigGetter[Array[AnyRef]] = new ConfigGetter[Array[AnyRef]] {
    override def get(config: Config, path: String): Option[Array[AnyRef]] = {
      _get[Array[AnyRef]](path)(x => config.getList(x).unwrapped().toArray())
    }
  }

  private[setl] implicit val StorageGetter: ConfigGetter[Storage] = new ConfigGetter[Storage] {
    override def get(config: Config, path: String): Option[Storage] = {
      _get[Storage](path)(x => Storage.valueOf(config.getString(x)))
    }
  }

  def getList(config: Config, path: String): Option[Array[AnyRef]] = {
    listGetter.get(config, path)
  }

  def getMap(config: Config): Map[String, String] = {
    import scala.collection.JavaConverters._
    config.entrySet().asScala.map(x => x.getKey -> x.getValue.unwrapped().toString).toMap
  }

  def isDefined(config: Config, path: String): Boolean = {
    try {
      config.getAnyRef(path) != null
    } catch {
      case _: ConfigException => false
    }
  }

  private[setl] trait ConfigGetter[T] {
    def get(config: Config, path: String): Option[T]
  }

}
