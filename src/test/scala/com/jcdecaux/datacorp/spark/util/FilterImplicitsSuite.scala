package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.enums.ValueType
import com.jcdecaux.datacorp.spark.storage.{Condition, Filter}
import org.scalatest.FunSuite

class FilterImplicitsSuite extends FunSuite {

  import FilterImplicits._

  test("Datetime/date filter to SQL request") {

    val datetimeFilter = Filter("dt", "<=", "datetime", Some("2019-01-01 00:00:05.1"))
    assert(datetimeFilter.toSqlRequest === "dt <= cast('2019-01-01 00:00:05' as timestamp)")

    val datetimeFilter2 = Filter("dt", ">", "datetime", Some("2019-01-01"))
    val datetimeFilter21 = Filter("dt", ">", "datetime", Some("2019-01-01 00:00:00"))
    assert(datetimeFilter2.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")
    assert(datetimeFilter21.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")

    val datetimeFilter3 = Filter("dt", ">", "date", Some("2019-01-01"))
    assert(datetimeFilter3.toSqlRequest === "dt > cast('2019-01-01' as date)")

    val datetimeFilter4 = Filter("dt", ">", "date", Some("2019-01-01 00:00:05.1"))
    val datetimeFilter41 = Filter("ddt", "<", "date", Some("2019-01-01 01:00:05.1"))
    val datetimeFilter42 = Filter("ddt", "<=", "date", Some("2019-01-01"))
    val datetimeFilter43 = Filter("ddt", "<", "datetime", Some("2019-01-01 02:00:05.1"))
    val datetimeFilter44 = Filter("ddt", "<=", "datetime", Some("2019-01-01"))
    assert(datetimeFilter4.toSqlRequest === "dt > cast('2019-01-01' as date)")
    assert(datetimeFilter41.toSqlRequest === "ddt < cast('2019-01-01' as date)")
    assert(datetimeFilter42.toSqlRequest === "ddt <= cast('2019-01-01' as date)")
    assert(datetimeFilter43.toSqlRequest === "ddt < cast('2019-01-01 02:00:05' as timestamp)")
    assert(datetimeFilter44.toSqlRequest === "ddt <= cast('2019-01-01 23:59:59' as timestamp)")

    // Should thrown exception
    val datetimeFilter5 = Filter("dt", "=", "datetime", Some("hahahaha"))
    assertThrows[IllegalArgumentException](datetimeFilter5.toSqlRequest)
  }

  test("String filter to SQL request") {
    val stringFilter = Filter("str", "=", "string", Some("hehe"))
    assert(stringFilter.toSqlRequest === "str = 'hehe'")
  }

  test("Numeric filter to SQL request") {
    val numFilter = Filter("other", ">", "int", Some("12"))
    assert(numFilter.toSqlRequest === "other > 12")
  }


  test("Filters to cql request") {
    val testFilters2: Set[Filter] = Set(
      Filter("datetime", ">", "datetime", Some("2019-01-01 20:00:05.1")),
      Filter("datetime2", "<=", "datetime", Some("2019-01-01")),
      Filter("other", ">", "int", Some("12")),
      Filter("str", "=", "string", Some("hehe"))
    )
    assert(testFilters2.toSqlRequest === "datetime >" +
      " cast('2019-01-01 20:00:05' as timestamp) AND" +
      " datetime2 <= cast('2019-01-01 23:59:59' as timestamp) AND" +
      " other > 12 AND" +
      " str = 'hehe'")

    val assetFilters: Set[Filter] = Set(
      Filter("country", "=", "string", Some("HK")),
      Filter("asset", "=", "string", Some("asset-8"))
    )
    assert(assetFilters.toSqlRequest === "country = 'HK' AND asset = 'asset-8'")
  }

  test("Filter to condition") {
    val datetimeFilter = Filter("dt", "<=", "datetime", Some("2019-01-01 00:00:05.1")).toCondition
    assert(datetimeFilter.toSqlRequest === "dt <= cast('2019-01-01 00:00:05' as timestamp)")

    val datetimeFilter2 = Filter("dt", ">", "datetime", Some("2019-01-01")).toCondition
    val datetimeFilter21 = Filter("dt", ">", "datetime", Some("2019-01-01 00:00:00")).toCondition
    assert(datetimeFilter2.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")
    assert(datetimeFilter21.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")

    val stringFilter = Filter("str", "=", "string", Some("hehe"))
    assert(stringFilter.toSqlRequest === "str = 'hehe'")
  }

  test("Datetime/date condition to SQL request") {

    val datetimeFilter = Condition("dt", "<=", Some("2019-01-01 00:00:05.1"), ValueType.DATETIME)
    assert(datetimeFilter.toSqlRequest === "dt <= cast('2019-01-01 00:00:05' as timestamp)")

    val datetimeFilter2 = Condition("dt", ">", Some("2019-01-01"), ValueType.DATETIME)
    val datetimeFilter21 = Condition("dt", ">", Some("2019-01-01 00:00:00"), ValueType.DATETIME)
    assert(datetimeFilter2.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")
    assert(datetimeFilter21.toSqlRequest === "dt > cast('2019-01-01 00:00:00' as timestamp)")

    val datetimeFilter3 = Condition("dt", ">", Some("2019-01-01"), ValueType.DATE)
    assert(datetimeFilter3.toSqlRequest === "dt > cast('2019-01-01' as date)")

    val datetimeFilter4 = Condition("dt", ">", Some("2019-01-01 00:00:05.1"), ValueType.DATE)
    val datetimeFilter41 = Condition("ddt", "<", Some("2019-01-01 01:00:05.1"), ValueType.DATE)
    val datetimeFilter42 = Condition("ddt", "<=", Some("2019-01-01"), ValueType.DATE)
    val datetimeFilter43 = Condition("ddt", "<", Some("2019-01-01 02:00:05.1"), ValueType.DATETIME)
    val datetimeFilter44 = Condition("ddt", "<=", Some("2019-01-01"), ValueType.DATETIME)
    assert(datetimeFilter4.toSqlRequest === "dt > cast('2019-01-01' as date)")
    assert(datetimeFilter41.toSqlRequest === "ddt < cast('2019-01-01' as date)")
    assert(datetimeFilter42.toSqlRequest === "ddt <= cast('2019-01-01' as date)")
    assert(datetimeFilter43.toSqlRequest === "ddt < cast('2019-01-01 02:00:05' as timestamp)")
    assert(datetimeFilter44.toSqlRequest === "ddt <= cast('2019-01-01 23:59:59' as timestamp)")

    // Should thrown exception
    val datetimeFilter5 = Condition("dt", "=", Some("hahahaha"), ValueType.DATETIME)
    assertThrows[IllegalArgumentException](datetimeFilter5.toSqlRequest)
  }

  test("String condition to SQL request") {
    val stringFilter = Condition("str", "=", Some("hehe"), ValueType.STRING)
    assert(stringFilter.toSqlRequest === "str = 'hehe'")
  }

  test("Numeric condition to SQL request") {
    val numFilter = Condition("other", ">", Some("12"), ValueType.NUMBER)
    assert(numFilter.toSqlRequest === "other > 12")
  }


  test("conditions to cql request") {
    val testFilters2: Set[Condition] = Set(
      Condition("datetime", ">", Some("2019-01-01 20:00:05.1"), ValueType.DATETIME),
      Condition("datetime2", "<=", Some("2019-01-01"), ValueType.DATETIME),
      Condition("other", ">", Some("12"), ValueType.NUMBER),
      Condition("str", "=", Some("hehe"), ValueType.STRING)
    )
    assert(testFilters2.toSqlRequest === "datetime >" +
      " cast('2019-01-01 20:00:05' as timestamp) AND" +
      " datetime2 <= cast('2019-01-01 23:59:59' as timestamp) AND" +
      " other > 12 AND" +
      " str = 'hehe'")

    val assetFilters: Set[Condition] = Set(
      Condition("country", "=", Some("HK"), ValueType.STRING),
      Condition("asset", "=", Some("asset-8"), ValueType.STRING)
    )
    assert(assetFilters.toSqlRequest === "country = 'HK' AND asset = 'asset-8'")
  }
}
