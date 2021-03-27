package io.github.setl.util

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypesafeConfigUtilsSuite extends AnyFunSuite with Matchers {

  val config = ConfigFactory.load("test_priority.conf")
  import TypesafeConfigUtils._

  test("TypesafeConfigUtils should handle implicit type conversion") {
    assert(getAs[String](config, "test.string") === Option("abc"))
    assert(getAs[Int](config, "test.int") === Option(1))
    assert(getAs[Long](config, "test.long") === Option(2L))
    assert(getAs[Float](config, "test.float") === Option(3.1F))
    assert(getAs[Float](config, "test.float2") === Option(3.1F))
    assert(getAs[Double](config, "test.double") === Option(4.4D))
    assert(getAs[Boolean](config, "test.boolean") === Option(false))
    assert(getAs[Boolean](config, "test.boolean2") === Option(true))
    assert(getAs[Int](config, "test.non_existing") === None)
    assert(isDefined(config, "test.non_existing") === false)
    assert(isDefined(config, "test.string"))
  }

  test("TypesafeConfigUtils should handle list") {
    getList(config, "test.list").get should equal (Array(1, 2, 3))
    val expected = Array(1.2, 2, 3)
    getList(config, "test.listFloat").get should equal (expected)
    getList(config, "test.listString").get should equal (Array("1.2", "2", "3"))
  }

  test("TypesafeConfigUtils should handle map") {
    getMap(config.getConfig("test.map")) should equal (Map("v1" -> "a", "v2" -> "b"))

  }

  test("TypesafeConfigUtils exceptions") {
    assertThrows[com.typesafe.config.ConfigException.WrongType](getAs[Int](config, "test.string"))
  }

}
