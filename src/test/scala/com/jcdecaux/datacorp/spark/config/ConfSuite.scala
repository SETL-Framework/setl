package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.ConfException
import org.scalatest.FunSuite

class ConfSuite extends FunSuite {

  val conf = new Conf()


  test("Set Conf") {
    conf.set("string", "mystring")
    conf.set("int", 1234)
    conf.set("long", 1234L)
    conf.set("float", 1234F)
    conf.set("double", 1234D)
    conf.set("boolean", true)

    conf.set("stringArray", Array("1", "2", "3"))
    conf.set("intArray", Array(1, 2, 3))
    conf.set("longArray", Array(1L, 2L, 3L))
    conf.set("floatArray", Array(1F, 2F, 3F))
    conf.set("doubleArray", Array(1D, 2D, 3D))
    conf.set("booleanArray", Array(true, false, true))

    conf.set("storage", Storage.CASSANDRA)
    conf.set("wrong_storage", "cassandraaa")

  }

  test("customize serializer") {
    case class Test(a: String, b: String)

    import Conf.Serializer

    implicit val testLoader: Serializer[Test] = new Serializer[Test] {
      override def deserialize(v: String): Option[Test] = {
        val x = v.split(",")

        Option(Test(x(0), x(1)))
      }

      override def serialize(v: Test): String = s"${v.a},${v.b}"
    }

    val myTest = Test("1", "2")
    conf.set("mytest", myTest)
    assert(conf.getAs[Test]("mytest").get === myTest)
  }

  test("One should be able to construct a Conf from map") {
    val map = Conf.fromMap(Map("a" -> "A", "b" -> "B"))
    val map2 = Conf(Map("a" -> "A", "b" -> "B"))
    assert(map.get("a").get === "A")
    assert(map.get("b").get === "B")
    assert(map2.get("a").get === "A")
    assert(map2.get("b").get === "B")
  }

  test("Get existing Conf") {
    assert(conf.getAs[String]("string").get === "mystring")
    assert(conf.getAs[Int]("int").get === 1234)
    assert(conf.getAs[Long]("long").get === 1234L)
    assert(conf.getAs[Long]("int").get === 1234L)
    assert(conf.getAs[Float]("float").get === 1234F)
    assert(conf.getAs[Double]("double").get === 1234D)
    assert(conf.getAs[Boolean]("boolean").get === true)

    assert(conf.getAs[Array[String]]("stringArray").get === Array("1", "2", "3"))
    assert(conf.getAs[Array[Int]]("intArray").get === Array(1, 2, 3))
    assert(conf.getAs[Array[Long]]("longArray").get === Array(1L, 2L, 3L))
    assert(conf.getAs[Array[Float]]("floatArray").get === Array(1F, 2F, 3F))
    assert(conf.getAs[Array[Double]]("doubleArray").get === Array(1D, 2D, 3D))
    assert(conf.getAs[Array[Boolean]]("booleanArray").get === Array(true, false, true))

    assert(conf.getAs[Storage]("storage").get === Storage.CASSANDRA)
    assertThrows[ConfException.Format](conf.getAs[Storage]("wrong_storage"))
    assert(conf.get("none") === None)
  }

  test("Get undefined conf") {
    assert(conf.getAs[Array[String]]("_stringArray") === None)
    assert(conf.getAs[Array[Int]]("_intArray") === None)
    assert(conf.getAs[Array[Long]]("_longArray") === None)
    assert(conf.getAs[Array[Float]]("_floatArray") === None)
    assert(conf.getAs[Array[Double]]("_doubleArray") === None)
  }

  test("Test wrong type => throw exception") {
    assertThrows[ConfException.Format](conf.getAs[Array[Boolean]]("longArray"))
    assertThrows[ConfException.Format](conf.getAs[Array[Float]]("string"))
    assertThrows[ConfException.Format](conf.getAs[Array[Boolean]]("stringArray"))
  }
}
