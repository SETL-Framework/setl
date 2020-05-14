package com.jcdecaux.setl.storage.connector

import java.io.File

import com.jcdecaux.setl.config.{Conf, FileConnectorConf, Properties}
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.repository.SparkRepository
import com.jcdecaux.setl.{SparkSessionBuilder, SparkTestUtils, TestObject}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class DeltaConnectorSuite extends AnyFunSuite {

  val path: String = "src/test/resources/test delta"
  val saveMode: SaveMode = SaveMode.Append
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

  test("Instantiation of constructors") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    import spark.implicits._

    val connector = new DeltaConnector(FileConnectorConf.fromMap(options))
    connector.write(testTable.toDF)
    assert(connector.read().collect().length == testTable.length)
    connector.drop()

    val connector2 = new DeltaConnector(options)
    connector2.write(testTable.toDF)
    assert(connector2.read().collect().length == testTable.length)
    connector2.drop()

    val connector3 = new DeltaConnector(path, saveMode)
    connector3.write(testTable.toDF)
    assert(connector3.read().collect().length == testTable.length)
    connector3.drop()

    val connector4 = new DeltaConnector(Properties.parquetConfig)
    connector4.write(testTable.toDF)
    assert(connector4.read().collect().length == testTable.length)
    connector4.drop()

    val connector5 = new DeltaConnector(conf)
    connector5.write(testTable.toDF)
    assert(connector5.read().collect().length == testTable.length)
    connector5.drop()
  }

  test("test Delta connector update") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    val deltaConnector = new DeltaConnector(path, SaveMode.Append)

    import spark.implicits._

    val dff: Dataset[TestObject] = Seq(
      TestObject(3, "p3", "c3", 2L),
      TestObject(4, "p4", "c4", 4L)
    ).toDS()

    deltaConnector.write(testTable.toDF())
    deltaConnector.update(dff.toDF(), "partition1", "partition2", "clustering1")

    assert(deltaConnector.read().count() === 4)
    assert(deltaConnector.read().select("value").distinct().count() === 3)
    deltaConnector.drop()
  }

  test("test Delta connector vacuum") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .build()
      .get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    val deltaConnector = new DeltaConnector(path, SaveMode.Append)

    import spark.implicits._

    val dff: Dataset[TestObject] = Seq(
      TestObject(3, "p3", "c3", 2L),
      TestObject(4, "p4", "c4", 4L)
    ).toDS()

    deltaConnector.write(testTable.toDF())
    deltaConnector.update(dff.toDF(), "partition1", "partition2", "clustering1")

    val beforeVacuum = numberOfFiles(spark, path)
    deltaConnector.vacuum(0)
    val afterVacuum = numberOfFiles(spark, path)

    assert(beforeVacuum > afterVacuum)
    deltaConnector.drop()
  }

  private def numberOfFiles(spark: SparkSession, path: String) : Long = {
      val filePaths = ArrayBuffer[Path]()
      val files = FileSystem
        .get(spark.sparkContext.hadoopConfiguration)
        .listFiles(new Path(path), true)
      while (files.hasNext) {
        val status = files.next()
        if (status.isFile) {
          filePaths += status.getPath
        }
      }
      filePaths.toArray.length
  }

  test("test Delta connector delete") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    val deltaConnector = new DeltaConnector(path, SaveMode.Append)

    import spark.implicits._

    deltaConnector.write(testTable.toDF())
    deltaConnector.delete("partition1 = 1")

    assert(deltaConnector.read().count() === 2)
    assert(deltaConnector.read().filter($"partition1" === 1).count() === 0)
    deltaConnector.drop()
  }

  test("Delta connector should push down filter and select") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    val deltaConnector = new DeltaConnector(path, SaveMode.Overwrite)

    val repository = new SparkRepository[TestObject].setConnector(deltaConnector).persistReadData(true)
    import spark.implicits._

    val ds = testTable.toDS()
    deltaConnector.write(ds.toDF())
    val ds5 = repository.findBy(Condition("partition1", "=", 1)).select($"partition1".as[Long], $"partition2".as[String])

    val explain = ExplainCommand(ds5.queryExecution.logical, extended = false)
    assert(
      spark.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("; ")
        .contains("PushedFilters: [IsNotNull(partition1), EqualTo(partition1,1)], ReadSchema: struct<partition1:int,partition2:string,clustering1:string,value:bigint>"),
      "Filters should be pushed"
    )
    deltaConnector.drop()
  }

  test("test Delta connector with different file path") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    val deltaConnector = new DeltaConnector(path, SaveMode.Overwrite)
    import spark.implicits._

    val path1: String = new File("src/test/resources/test parquet").toURI.toString
    val path2: String = new File("src/test/resources/test parquet").getPath

    val deltaConnector1 = new ParquetConnector(path1, SaveMode.Overwrite)
    val deltaConnector2 = new ParquetConnector(path2, SaveMode.Overwrite)

    deltaConnector1.write(testTable.toDF)
    val df = deltaConnector2.read()
    assert(df.count() === 3)
    deltaConnector.drop()
  }

  test("test Delta connector partition by") {

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

    val deltaConnector = new DeltaConnector(path, SaveMode.Overwrite)
      .partitionBy("partition1", "partition2")

    deltaConnector.write(dff.toDF)
    assert(deltaConnector.read().count() === 6)
    assert(deltaConnector.read().columns.length === 4, "column suffix should not exists")
    deltaConnector.drop()
  }
}
