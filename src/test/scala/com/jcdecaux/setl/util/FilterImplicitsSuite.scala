package com.jcdecaux.setl.util

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.enums.ValueType
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.util.FilterImplicitsSuite.TestFilterImplicit
import org.scalatest.funsuite.AnyFunSuite

class FilterImplicitsSuite extends AnyFunSuite {

  import FilterImplicits._

  test("Datetime/date condition to SQL request") {

    val datetimeFilter = Condition("dt", "<=", Some("2019-01-01 00:00:05.1"), ValueType.DATETIME)
    assert(datetimeFilter.toSqlRequest === "(`dt` <= cast('2019-01-01 00:00:05' as timestamp))")

    val datetimeFilter2 = Condition("dt", ">", Some("2019-01-01"), ValueType.DATETIME)
    val datetimeFilter21 = Condition("dt", ">", Some("2019-01-01 00:00:00"), ValueType.DATETIME)
    assert(datetimeFilter2.toSqlRequest === "(`dt` > cast('2019-01-01 00:00:00' as timestamp))")
    assert(datetimeFilter21.toSqlRequest === "(`dt` > cast('2019-01-01 00:00:00' as timestamp))")

    val datetimeFilter3 = Condition("dt", ">", Some("2019-01-01"), ValueType.DATE)
    assert(datetimeFilter3.toSqlRequest === "(`dt` > cast('2019-01-01' as date))")

    val datetimeFilter4 = Condition("dt", ">", Some("2019-01-01 00:00:05.1"), ValueType.DATE)
    val datetimeFilter41 = Condition("ddt", "<", Some("2019-01-01 01:00:05.1"), ValueType.DATE)
    val datetimeFilter42 = Condition("ddt", "<=", Some("2019-01-01"), ValueType.DATE)
    val datetimeFilter43 = Condition("ddt", "<", Some("2019-01-01 02:00:05.1"), ValueType.DATETIME)
    val datetimeFilter44 = Condition("ddt", "<=", Some("2019-01-01"), ValueType.DATETIME)
    assert(datetimeFilter4.toSqlRequest === "(`dt` > cast('2019-01-01' as date))")
    assert(datetimeFilter41.toSqlRequest === "(`ddt` < cast('2019-01-01' as date))")
    assert(datetimeFilter42.toSqlRequest === "(`ddt` <= cast('2019-01-01' as date))")
    assert(datetimeFilter43.toSqlRequest === "(`ddt` < cast('2019-01-01 02:00:05' as timestamp))")
    assert(datetimeFilter44.toSqlRequest === "(`ddt` <= cast('2019-01-01 23:59:59' as timestamp))")

    // Should thrown exception
    val datetimeFilter5 = Condition("dt", "=", Some("hahahaha"), ValueType.DATETIME)
    assertThrows[IllegalArgumentException](datetimeFilter5.toSqlRequest)
  }

  test("String condition to SQL request") {
    val stringFilter = Condition("str", "=", Some("hehe"), ValueType.STRING)
    assert(stringFilter.toSqlRequest === "(`str` = 'hehe')")
  }

  test("Numeric condition to SQL request") {
    val numFilter = Condition("other", ">", Some("12"), ValueType.NUMBER)
    assert(numFilter.toSqlRequest === "(`other` > 12)")
  }


  test("conditions to cql request") {
    val testFilters2: Set[Condition] = Set(
      Condition("datetime", ">", Some("2019-01-01 20:00:05.1"), ValueType.DATETIME),
      Condition("datetime2", "<=", Some("2019-01-01"), ValueType.DATETIME),
      Condition("other", ">", Some("12"), ValueType.NUMBER),
      Condition("str", "=", Some("hehe"), ValueType.STRING)
    )
    assert(testFilters2.toSqlRequest === "(`datetime` > cast('2019-01-01 20:00:05' as timestamp)) AND" +
      " (`datetime2` <= cast('2019-01-01 23:59:59' as timestamp)) AND" +
      " (`other` > 12) AND" +
      " (`str` = 'hehe')")

    val assetFilters: Set[Condition] = Set(
      Condition("country", "=", Some("HK"), ValueType.STRING),
      Condition("asset", "=", Some("asset-8"), ValueType.STRING)
    )
    assert(assetFilters.toSqlRequest === "(`country` = 'HK') AND (`asset` = 'asset-8')")
  }

  test("Filter implicit should be applied to dataset") {

    val spark = new SparkSessionBuilder().setEnv("local").getOrCreate()
    import spark.implicits._
    val ds = spark.createDataset(
      Seq(
        TestFilterImplicit("a", "A"),
        TestFilterImplicit("b", "B"),
        TestFilterImplicit("c", "C")
      )
    )

    val df = ds.toDF()
    import FilterImplicits.DatasetFilterByCondition
    assert(ds.filter(Condition("col1", "=", "a")).count() === 1)
    assert(ds.filter(Set(Condition("col1", "=", "a"))).count() === 1)
    assert(df.filter(Condition("col1", "=", "a")).count() === 1)
    assert(ds.filter(Set(Condition("col1", "=", "a"), Condition("col2", "=", "A"))).count() === 1)
    assert(ds.filter(Set(Condition("col1", "=", "a"), Condition("col2", "=", "B"))).count() === 0)

    assert(ds.filter(Condition($"col1" === "a")).count() === 1)
    assertThrows[org.apache.spark.sql.AnalysisException](ds.filter(Condition($"col1" contains "a")).show())

  }
}

object FilterImplicitsSuite {

  case class TestFilterImplicit(col1: String, col2: String)

}
