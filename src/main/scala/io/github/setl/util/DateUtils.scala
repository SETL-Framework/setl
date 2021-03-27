package io.github.setl.util

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.joda.time.DateTime

import scala.util.matching.Regex

/**
 * DateUtil
 */
object DateUtils {
  /**
   * Regex that matches datetime with the following format:
   *
   * <ul>
   * <li><code>2018-01-01 00:00:45.100+0000</code></li>
   * <li><code>2018-01-01 00:00:45.100Z</code></li>
   * <li><code>2018-01-01T00:00:45Z</code></li>
   * <li><code>2018-01-01T00:00:45Z</code></li>
   * <li><code>2018-01-01T00:00:45</code></li>
   * </ul>
   *
   * <b>It doesn't check the validity of date, which means it can works on date like 9999-99-99 99:99:99</b>
   */
  val datetimePattern: Regex = "^(\\d{4})-(\\d{2})-(\\d{2})[ T]((\\d{2}):(\\d{2}):(\\d{2})(\\.\\d+)?)?(\\+(\\d{4})|Z)?$".r

  /**
   * Regex that matches date with the following format:
   * <ul>
   * <li><code>yyyy-MM-dd</code></li>
   * </ul>
   */
  val datePattern: Regex = "^\\d{4}-\\d{2}-\\d{2}$".r

  /**
   * Parse a given date string with the given pattern to a [[java.sql.Date]] object
   *
   * @param date     date string
   * @param pattern  format of the given date string
   * @param timeZone time zone of the given date string
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
   * Parse and round a given date string with the given pattern to a [[java.sql.Timestamp]] object
   *
   * @param date     date string
   * @param pattern  format of the given date string
   * @param timeZone time zone of the given date string
   * @return
   */
  def getFirstMinuteOfHour(date: String,
                           pattern: String = "yyyy-MM-dd HH:mm:ss",
                           timeZone: String = "UTC"): java.sql.Timestamp = {
    val datetime = new DateTime(getDatetimeFromString(date, pattern, timeZone))
    new java.sql.Timestamp(datetime.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).toDate.getTime)
  }

  /**
   * Parse a given date string with the given pattern to a [[java.sql.Timestamp]] object
   *
   * @param date     date string
   * @param pattern  format of the given date string
   * @param timeZone time zone of the given date string
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
   * From a given date or datetime string, retrieve the correct date format (with or without time)
   *
   * <strong>Note: the maximum precision is 1 second</strong>
   *
   * @param input    your date/datetime string
   * @param withTime true to return a datetime, false to return only a date
   * @param end      true to return the last seconds of the given date, false to return the first second
   *                 (<b>this only takes effect when there is no time in the given input</b>)
   * @return
   */
  @throws[IllegalArgumentException]
  def reformatDateTimeString(input: String, withTime: Boolean, end: Boolean): String = {

    val date: String = input.trim match {
      case datePattern() => input
      case datetimePattern(year, month, day, _, _, _, _, _, _, _) => s"$year-$month-$day"
      case _ => throw new IllegalArgumentException("Wrong date/datetime format")
    }


    if (withTime) {

      val time: String = input.trim match {
        case datePattern() => if (end) "23:59:59" else "00:00:00"
        case datetimePattern(_, _, _, _, hour, min, sec, _, _, _) => s"$hour:$min:$sec"
        case _ => throw new IllegalArgumentException("Wrong date/datetime format")
      }

      s"$date $time"
    } else {
      date
    }
  }
}
