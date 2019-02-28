package com.jcdecaux.datacorp.spark.util

import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.util.matching.Regex

import org.joda.time.DateTime

/**
  * DateUtil
  */
object DateUtils {

  /**
    *
    * @param date
    * @param pattern
    * @param timeZone
    * @return
    */
  def getDatetimeFromString(date: String,
                            pattern: String = "yyyy-MM-dd HH:mm:ss",
                            timeZone: String = "UTC"): java.sql.Timestamp = {
    val formatter = new SimpleDateFormat(pattern)
    formatter.setTimeZone(TimeZone.getTimeZone(timeZone))
    new java.sql.Timestamp(formatter.parse(date).getTime)
  }

  /**
    *
    * @param date
    * @param pattern
    * @param timeZone
    * @return
    */
  def getDateFromString(date: String,
                        pattern: String = "yyyy-MM-dd",
                        timeZone: String = "UTC"): java.sql.Date = {
    val formatter = new SimpleDateFormat(pattern)
    formatter.setTimeZone(TimeZone.getTimeZone(timeZone))
    new java.sql.Date(formatter.parse(date).getTime)
  }

  /**
    *
    * @param date
    * @param pattern
    * @param timeZone
    * @return
    */
  def getFirstMinuteOfHour(date: String,
                           pattern: String = "yyyy-MM-dd HH:mm:ss",
                           timeZone: String = "UTC"): java.sql.Timestamp = {
    val datetime = new DateTime(getDatetimeFromString(date, pattern, timeZone))
    new java.sql.Timestamp(datetime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate.getTime)
  }

  val datetimePattern: Regex = "^(\\d{4})-(\\d{2})-(\\d{2})(\\ (\\d{2}):(\\d{2}):(\\d{2})(\\.\\d+)?)?(\\+(\\d{4}))?$".r
  val datePattern: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r

  /**
    * From a given date or datetime string, retrieve the correct date format (with or without time)
    *
    * <strong>Note: the maximum precision is 1 second</strong>
    *
    * @param input    your date/datetime string
    * @param withTime true to return a datetime, false to return only a date
    * @return
    */
  def matchDateTime(input: String, withTime: Boolean): String = {
    input match {
      case datePattern() =>
        if (withTime) {
          s"$input 00:00:00"
        } else {
          input
        }

      case datetimePattern(year, month, day, _, hour, min, sec, _, _, tz) =>
        if (withTime) {
          s"$year-$month-$day $hour:$min:$sec"
        } else {
          s"$year-$month-$day"
        }

      case _ => throw new NumberFormatException("Wrong date/datetime format")
    }
  }
}
