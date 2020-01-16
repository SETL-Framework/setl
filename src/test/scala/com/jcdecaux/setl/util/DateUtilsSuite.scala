package com.jcdecaux.setl.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.TimeZone

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
    assertThrows[IllegalArgumentException](DateUtils.reformatDateTimeString("2019-01 00:00:00", withTime = true, end = false))

    assert(DateUtils.reformatDateTimeString(dt8, withTime = false, end = true) === "2019-01-01")
    assert(DateUtils.reformatDateTimeString(dt8, withTime = true, end = true) === "2019-01-01 23:55:10")
    assert(DateUtils.reformatDateTimeString(dt8, withTime = true, end = false) === "2019-01-01 23:55:10")
  }

  test("DateUtils.getDateFromString") {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

    val test = DateUtils.getDateFromString("1990-11-11", "yyyy-MM-dd", "UTC")
    assert(formatter.format(test) === "1990-11-11")
  }

  test("DateUtils.getDatetimeFromString") {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

    val test = DateUtils.getDatetimeFromString("1990-11-11", "yyyy-MM-dd", "UTC")
    val test2 = DateUtils.getDatetimeFromString("1990-11-11", "yyyy-MM-dd", "Europe/Paris")
    assert(formatter.format(test) === "1990-11-11 00:00:00")
    assert(formatter.format(test2) === "1990-11-10 23:00:00")
  }

  test("DateUtils.getFirstMinuteOfHour") {
    val test = DateUtils.getFirstMinuteOfHour("1990-11-11", "yyyy-MM-dd", "UTC")
    val test2 = DateUtils.getFirstMinuteOfHour("1990-11-11 00:12:21", "yyyy-MM-dd HH:mm:ss", "UTC")
    val test3 = DateUtils.getFirstMinuteOfHour("1990-11-11 01:12:21", "yyyy-MM-dd HH:mm:ss", "Europe/Paris")

    assert(test === test2)
    assert(test3 === test2)
  }

}
