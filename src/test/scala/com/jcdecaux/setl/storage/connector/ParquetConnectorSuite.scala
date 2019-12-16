package com.jcdecaux.setl.storage.connector

import java.io.File

import com.jcdecaux.setl.config.Properties
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ParquetConnectorSuite extends AnyFunSuite {

  val path: String = "src/test/resources/test parquet"
  val table: String = "test_table"

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  test("Parquet connector should push down filter and select") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)

    val repository = new SparkRepository[TestObject].setConnector(parquetConnector).persistReadData(true)
    import spark.implicits._

    val ds = testTable.toDS()
    parquetConnector.write(ds.toDF())

    val ds2 = parquetConnector.read().filter("partition1 = 1").as[TestObject].select($"partition1".as[Long])
    ds2.show()
    ds2.explain()

    val ds3 = repository.findBy(Condition("partition1", "=", 1)).select("partition1")
    ds3.show()
    ds3.explain()

    val ds4 = repository.findBy(Condition("partition1", "=", 1)).select("partition1")
    ds4.explain()

    val ds5 = repository.findBy(Condition("partition1", "=", 1)).select($"partition1".as[Long], $"partition2".as[String])
    ds5.show()
    ds5.explain()

    val explain = ExplainCommand(ds5.queryExecution.logical, extended = false)
    assert(
      spark.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("; ")
        .contains("PushedFilters: [IsNotNull(partition1), EqualTo(partition1,1)], ReadSchema: struct<partition1:int,partition2:string,clustering1:string,value:bigint>"),
      "Filters should be pushed"
    )
    parquetConnector.delete()
  }

  test("test Parquet connector with different file path") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)
    import spark.implicits._

    val path1: String = new File("src/test/resources/test parquet").toURI.toString
    val path2: String = new File("src/test/resources/test parquet").getPath

    val parquetConnector1 = new ParquetConnector(path1, SaveMode.Overwrite)
    val parquetConnector2 = new ParquetConnector(path2, SaveMode.Overwrite)

    parquetConnector1.write(testTable.toDF)
    val df = parquetConnector2.read()
    assert(df.count() === 3)
    parquetConnector.delete()
  }

  test("parquet connector  IO ") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)
    import spark.implicits._

    testTable.toDF.show()
    parquetConnector.write(testTable.toDF())
    parquetConnector.write(testTable.toDF())

    val df = parquetConnector.read()
    df.show()
    assert(df.count() === 3)
    parquetConnector.delete()
  }

  test("IO with auxiliary parquet connector constructor") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new ParquetConnector(Properties.parquetConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 6)
    connector.delete()
  }

  test("Test Parquet Connector Suffix") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)
    import spark.implicits._

    parquetConnector.resetSuffix(true)
    parquetConnector.write(testTable.toDF(), Some("2"))
    parquetConnector.write(testTable.toDF(), Some("1"))
    parquetConnector.write(testTable.toDF(), Some("3"))

    val df = parquetConnector.read()
    df.show()
    assert(df.count() == 9)
    assert(df.filter($"partition1" === 1).count() === 3)
    assert(df.filter($"partition1" === 1).dropDuplicates().count() === 1)

    parquetConnector.delete()
    assertThrows[org.apache.spark.sql.AnalysisException](parquetConnector.read())
  }

  test("test partition by") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val dff: Dataset[TestObject] = Seq(
      TestObject(1, "p1", "c1", 1L),
      TestObject(2, "p2", "c2", 2L),
      TestObject(2, "p1", "c2", 2L),
      TestObject(3, "p3", "c3", 3L),
      TestObject(3, "p2", "c3", 3L),
      TestObject(3, "p3", "c3", 3L)
    ).toDS()

    val parquetConnector2 = new ParquetConnector(path, SaveMode.Overwrite)
      .partitionBy("partition1", "partition2")

    // with partition, with suffix
    parquetConnector2.write(dff.toDF, Some("1"))
    parquetConnector2.write(dff.toDF, Some("2"))
    parquetConnector2.dropUserDefinedSuffix(false)

    parquetConnector2.read().show()
    assert(parquetConnector2.read().count() === 12)
    assert(parquetConnector2.read().columns.length === 5)
    parquetConnector2.delete()

    // with partition without suffix
    parquetConnector2.resetSuffix(true)
    parquetConnector2.write(dff.toDF)
    assert(parquetConnector2.read().count() === 6)
    assert(parquetConnector2.read().columns.length === 4, "column suffix should not exists")
    parquetConnector2.dropUserDefinedSuffix(true)
    assert(parquetConnector2.read().columns.length === 4, "column suffix should not exists")
    parquetConnector2.delete()
  }

}
