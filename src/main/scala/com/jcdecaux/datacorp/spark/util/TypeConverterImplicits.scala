package com.jcdecaux.datacorp.spark.util

trait TypeConverterImplicits {

  trait TypeConverter[T] {

    def convert(v: String): Option[T]

  }

  object TypeConverter {

    implicit val stringLoader: TypeConverter[String] = new TypeConverter[String] {
      override def convert(v: String): Option[String] = Some(v)
    }

    implicit val intLoader: TypeConverter[Int] = new TypeConverter[Int] {
      override def convert(v: String): Option[Int] = if (v != null) Some(v.toInt) else None
    }

    implicit val longLoader: TypeConverter[Long] = new TypeConverter[Long] {
      override def convert(v: String): Option[Long] = if (v != null) Some(v.toLong) else None
    }

    implicit val floatLoader: TypeConverter[Float] = new TypeConverter[Float] {
      override def convert(v: String): Option[Float] = if (v != null) Some(v.toFloat) else None
    }

    implicit val doubleLoader: TypeConverter[Double] = new TypeConverter[Double] {
      override def convert(v: String): Option[Double] = if (v != null) Some(v.toDouble) else None
    }

    implicit val booleanLoader: TypeConverter[Boolean] = new TypeConverter[Boolean] {
      override def convert(v: String): Option[Boolean] = if (v != null) Some(v.toBoolean) else None
    }

  }

}
