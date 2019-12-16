package com.jcdecaux.setl.util

import org.scalatest.funsuite.AnyFunSuite

class DateUtilsSuite extends AnyFunSuite {

  test("ReformatTimeFromString") {

    val dt1 = "2019-11-01 23:55:10.0+0000"
    val dt2 = "2019-01-01 23:55:10.0Z"
    val dt3 = "2019-01-01 23:55:10Z"
    val dt4 = "2019-01-01 23:55:10+0000"
    val dt41 = "2019/01/01 23:55:10+0000"
    val dt42 = "01/01/2019 23:55:10+0000"

    val dt5 = "2019-01-01"
    val dt6 = "2019-01-01 qdfsq"
    val dt7 = "2019-01-01 23:55:10R"
    val dt8 = "2019-01-01 23:55:10 "

    assert(DateUtils.reformatDateTimeString(dt1, withTime = true, end = true) === "2019-11-01 23:55:10")
    assert(DateUtils.reformatDateTimeString(dt1, withTime = false, end = true) === "2019-11-01")

    assert(DateUtils.reformatDateTimeString(dt2, withTime = false, end = true) === "2019-01-01")
    assert(DateUtils.reformatDateTimeString(dt2, withTime = true, end = true) === "2019-01-01 23:55:10")

    assert(DateUtils.reformatDateTimeString(dt3, withTime = true, end = true) === "2019-01-01 23:55:10")
    assert(DateUtils.reformatDateTimeString(dt3, withTime = false, end = true) === "2019-01-01")

    assert(DateUtils.reformatDateTimeString(dt4, withTime = true, end = true) === "2019-01-01 23:55:10")
    assert(DateUtils.reformatDateTimeString(dt4, withTime = false, end = true) === "2019-01-01")

    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt41, withTime = true, end = false))
    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt41, withTime = false, end = false))

    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt42, withTime = true, end = false))
    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt42, withTime = false, end = false))

    assert(DateUtils.reformatDateTimeString(dt5, withTime = false, end = true) === "2019-01-01")
    assert(DateUtils.reformatDateTimeString(dt5, withTime = true, end = true) === "2019-01-01 23:59:59")
    assert(DateUtils.reformatDateTimeString(dt5, withTime = true, end = false) === "2019-01-01 00:00:00")

    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt6, withTime = true, end = false))
    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString(dt7, withTime = true, end = false))

    assert(DateUtils.reformatDateTimeString(dt8, withTime = false, end = true) === "2019-01-01")
    assert(DateUtils.reformatDateTimeString(dt8, withTime = true, end = true) === "2019-01-01 23:55:10")
    assert(DateUtils.reformatDateTimeString(dt8, withTime = true, end = false) === "2019-01-01 23:55:10")
  }

}
