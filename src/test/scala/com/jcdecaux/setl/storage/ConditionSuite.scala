package com.jcdecaux.setl.storage

import java.time.{LocalDate, LocalDateTime}

import com.jcdecaux.setl.enums.ValueType
import org.scalatest.funsuite.AnyFunSuite

class ConditionSuite extends AnyFunSuite {

  test("Condition could be converted to sql request") {

    val strCond = Condition("col1", "=", "haha")
    assert(strCond.toSqlRequest === "(`col1` = 'haha')")

    val intCond = Condition("col1", "=", 1)
    assert(intCond.toSqlRequest === "(`col1` = 1)")

    val floatCond = Condition("col1", "=", 1F)
    assert(floatCond.toSqlRequest === "(`col1` = 1.0)")

    val date = LocalDate.parse("1990-01-01")
    val dateCond = Condition("date", "=", date)
    assert(dateCond.toSqlRequest === "(`date` = cast('1990-01-01' as date))")

    val datetime = LocalDateTime.parse("1990-01-01T00:00:00")
    val datetimeCond = Condition("datetime", "=", datetime)
    assert(datetimeCond.toSqlRequest === "(`datetime` = cast('1990-01-01 00:00:00' as timestamp))")

    val strSetCond = Condition("str_set", "in", Set("a", "b"))
    assert(strSetCond.toSqlRequest === "(`str_set` IN ('a', 'b'))")

    val floatSetCond = Condition("float_set", "in", Set(1.343F, 2.445F))
    assert(floatSetCond.toSqlRequest === "(`float_set` IN (1.343, 2.445))")

    val strCondWithType = Condition("col1", "=", "hehe", ValueType.STRING)
    assert(strCondWithType.toSqlRequest === "(`col1` = 'hehe')")
  }

  test("Condition should return null if value is not defined") {
    val cond = Condition("a", "=", None, ValueType.STRING)
    assert(cond.toSqlRequest === null)
  }

  test("Null sql request should be ignored in a condition set") {

    val conds = Set(
      Condition("a", "=", None, ValueType.STRING),
      Condition("b", "=", 1.5),
      Condition("c", "in", Set("x", "y"))
    )

    import com.jcdecaux.setl.util.FilterImplicits._
    assert(conds.toSqlRequest === "(`b` = 1.5) AND (`c` IN ('x', 'y'))")

  }

  test("Condition should handle Column") {
    import org.apache.spark.sql.functions._
    val condition = Condition(
      col("test").isin(1, 2, 3)
    )
    assert(condition.toSqlRequest === Condition("test", "IN", Set(1, 2, 3)).toSqlRequest)

    val condition2 = Condition(
      col("test").isin(1, 2, 3) && col("test2") === "A"
    )
    assert(condition2.toSqlRequest === "((`test` IN (1, 2, 3)) AND (`test2` = 'A'))")
  }
}
