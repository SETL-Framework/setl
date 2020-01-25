package com.jcdecaux.setl.storage.connector

import java.io.File

import com.jcdecaux.setl.config.{Conf, FileConnectorConf, Properties}
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.{SparkSessionBuilder, TestObject}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ParquetConnectorSuite extends AnyFunSuite {

  val path: String = "src/test/resources/test parquet"
  val saveMode: SaveMode = SaveMode.Overwrite
  val table: String = "test_table"

  val options: Map[String, String] = Map[String, String](
    "path" -> path,
    "saveMode" -> saveMode.toString
  )

  val conf: Conf = new Conf()
  conf.set(options)

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  test("Instanciation of constructors") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new ParquetConnector(FileConnectorConf.fromMap(options))
    val connector2 = new ParquetConnector(spark, FileConnectorConf.fromMap(options))
    connector.write(testTable.toDF)
    assert(connector.read().collect().length == testTable.length)
    assert(connector2.read().collect().length == testTable.length)
    connector.delete()
    connector2.delete()

    val connector3 = new ParquetConnector(options)
    val connector4 = new ParquetConnector(spark, options)
    connector3.write(testTable.toDF)
    assert(connector3.read().collect().length == testTable.length)
    assert(connector4.read().collect().length == testTable.length)
    connector3.delete()
    connector4.delete()

    val connector5 = new ParquetConnector(path, saveMode)
    val connector6 = new ParquetConnector(spark, path, saveMode)
    connector5.write(testTable.toDF)
    assert(connector5.read().collect().length == testTable.length)
    assert(connector6.read().collect().length == testTable.length)
    connector5.delete()
    connector6.delete()

    val connector7 = new ParquetConnector(Properties.parquetConfig)
    val connector8 = new ParquetConnector(spark, Properties.parquetConfig)
    connector7.write(testTable.toDF)
    assert(connector7.read().collect().length == testTable.length)
    assert(connector8.read().collect().length == testTable.length)
    connector7.delete()
    connector8.delete()

    val connector9 = new ParquetConnector(conf)
    val connector10 = new ParquetConnector(spark, conf)
    connector9.write(testTable.toDF)
    assert(connector9.read().collect().length == testTable.length)
    assert(connector10.read().collect().length == testTable.length)
    connector9.delete()
    connector10.delete()
  }

  test("Parquet connector should push down filter and select") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)

    // test legacy constructor
    new ParquetConnector(spark, path, SaveMode.Overwrite)

    val repository = new SparkRepository[TestObject].setConnector(parquetConnector).persistReadData(true)
    import spark.implicits._

    val ds = testTable.toDS()
    parquetConnector.write(ds.toDF())
    val ds2 = parquetConnector.read().filter("partition1 = 1").as[TestObject].select($"partition1".as[Long])
    val ds3 = repository.findBy(Condition("partition1", "=", 1)).select("partition1")
    val ds4 = repository.findBy(Condition("partition1", "=", 1)).select("partition1")
    val ds5 = repository.findBy(Condition("partition1", "=", 1)).select($"partition1".as[Long], $"partition2".as[String])

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

    parquetConnector.write(testTable.toDF())
    parquetConnector.write(testTable.toDF())

    val df = parquetConnector.read()
    assert(df.count() === 3)
    parquetConnector.delete()
  }

  test("IO with auxiliary parquet connector constructor") {

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new ParquetConnector(Properties.parquetConfig)

    // test legacy constructor
    new  ParquetConnector(spark, Properties.parquetConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
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
