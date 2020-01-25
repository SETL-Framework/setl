package com.jcdecaux.setl.storage.connector

import java.io.File

import com.jcdecaux.setl.config.{Conf, FileConnectorConf, Properties}
import com.jcdecaux.setl.{SparkSessionBuilder, TestObject}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JSONConnectorSuite extends AnyFunSuite with Matchers {

  val path: String = "src/test/resources/test JSON"

  val standardPath: String = path + ".json"

  val options: Map[String, String] = Map[String, String](
    "path" -> path,
    "saveMode" -> "Overwrite"
  )

  val conf: Conf = new Conf()
  conf.set(options)

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  test("Instanciation of constructors") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new JSONConnector(FileConnectorConf.fromMap(options))
    connector.write(testTable.toDF())
    val connector2 = new JSONConnector(spark, FileConnectorConf.fromMap(options))
    assert(connector.read().collect().length == testTable.length)
    assert(connector2.read().collect().length == testTable.length)
    connector.delete()
    connector2.delete()

    val connector3 = new JSONConnector(options)
    val connector4 = new JSONConnector(spark, options)
    connector3.write(testTable.toDF())
    assert(connector3.read().collect().length == testTable.length)
    assert(connector4.read().collect().length == testTable.length)
    connector3.delete()
    connector4.delete()

    val connector5 = new JSONConnector(Properties.jsonConfig)
    val connector6 = new JSONConnector(spark, Properties.jsonConfig)
    connector5.write(testTable.toDF())
    assert(connector5.read().collect().length == testTable.length)
    assert(connector6.read().collect().length == testTable.length)
    connector5.delete()
    connector6.delete()

    val connector7 = new JSONConnector(conf)
    val connector8 = new JSONConnector(spark, conf)
    connector7.write(testTable.toDF())
    assert(connector7.read().collect().length == testTable.length)
    assert(connector8.read().collect().length == testTable.length)
    connector7.delete()
    connector8.delete()
  }

  test("JSON should give correct path") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val conf = new Conf()
    conf.set("path", path)

    val connector = new JSONConnector(conf)
    assert(connector.getStandardJSONPath == new Path(standardPath))

    val conf2 = new Conf()
    conf2.set("path", standardPath)
    val connector2 = new JSONConnector(conf2)
    assert(connector2.getStandardJSONPath == new Path(standardPath))

    connector.delete()
    connector2.delete()
  }

  test("JSON connector IO") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val connector = new JSONConnector(Map("path" -> path, "saveMode" -> "Overwrite"))

    import spark.implicits._

    connector.partitionBy("partition1").write(testTable.toDF())
    val df = connector.read()
    assert(df.count() === 6)
    assert(df.filter($"partition1" === 2).count() === 4)
    connector.delete()
  }

  test("test JSON connector with different file path") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val path1: String = new File("src/test/resources/test_json").toURI.toString
    val path2: String = new File("src/test/resources/test_json").getPath

    val connector1 = new JSONConnector(Map[String, String]("path" -> path1, "saveMode" -> "Overwrite"))
    val connector2 = new JSONConnector(Map[String, String]("path" -> path2, "saveMode" -> "Overwrite"))

    connector1.write(testTable.toDF)
    val df = connector2.read()
    assert(df.count() === 6)
    connector1.delete()
  }

  test("IO with auxiliary JSONConnector constructor") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new JSONConnector(Properties.jsonConfig)

    connector.write(testTable.toDF())
    connector.write(testTable.toDF())

    val df = connector.read()
    df.show()
    assert(df.count() === 12)
    connector.delete()
  }

  test("Test JSON Connector Suffix") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new JSONConnector(Map("path" -> path, "saveMode" -> "Append"))

    connector.write(testTable.toDF(), Some("2"))
    connector.write(testTable.toDF(), Some("2"))
    connector.write(testTable.toDF(), Some("1"))
    connector.write(testTable.toDF(), Some("3"))

    val df = connector.read()
    df.show()
    assert(df.count() == 24)
    assert(df.filter($"partition1" === 1).count() === 4)
    assert(df.filter($"partition1" === 1).dropDuplicates().count() === 1)

    connector.delete()
    assertThrows[org.apache.spark.sql.AnalysisException](connector.read())
  }

  test("test JSON partition by") {
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

    val connector = new JSONConnector(Map[String, String]("path" -> path, "saveMode" -> "Overwrite"))
      .partitionBy("partition1", "partition2")

    // with partition, with suffix
    connector.write(dff.toDF, Some("1"))
    connector.write(dff.toDF, Some("2"))
    connector.dropUserDefinedSuffix(false)

    connector.read().show()
    assert(connector.read().count() === 12)
    assert(connector.read().columns.length === 5)
    connector.delete()

    // with partition without suffix
    connector.resetSuffix(true)
    connector.write(dff.toDF)
    assert(connector.read().count() === 6)
    assert(connector.read().columns.length === 4, "column suffix should not exists")
    connector.dropUserDefinedSuffix(true)
    assert(connector.read().columns.length === 4, "column suffix should not exists")
    connector.delete()
  }

  test("Complex JSON file") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val connector = new JSONConnector(Map("path" -> "src/test/resources/test-json.json", "saveMode" -> "Append", "multiLine" -> "true"))
    connector.read()
      .select(functions.col("col3.col3-2").as[String])
      .collect() should contain theSameElementsAs Array("hehe", "hehehehe", "hehehehehehe", "hehehehehehehehe")
  }

  test("JSONConnector should be able to write standard JSON format") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val expectedResult = "[{\"partition1\":1,\"partition2\":\"p1\",\"clustering1\":\"c1\",\"value\":1},{\"partition1\":2,\"partition2\":\"p2\",\"clustering1\":\"c2\",\"value\":2},{\"partition1\":2,\"partition2\":\"p2\",\"clustering1\":\"c2\",\"value\":2},{\"partition1\":2,\"partition2\":\"p2\",\"clustering1\":\"c2\",\"value\":2},{\"partition1\":2,\"partition2\":\"p2\",\"clustering1\":\"c2\",\"value\":2},{\"partition1\":3,\"partition2\":\"p3\",\"clustering1\":\"c3\",\"value\":3}]"

    val path1: String = new File("src/test/resources/standart_json_format").toURI.toString
    val connector1 = new JSONConnector(Map[String, String]("path" -> path1, "saveMode" -> "Overwrite"))
    connector1.writeStandardJSON(testTable.toDF)
    assert(connector1.readStandardJSON() === expectedResult)
    connector1.deleteStandardJSON()

    val connector2 = new JSONConnector(Map[String, String]("path" -> path1, "saveMode" -> "Append"))
    connector2.writeStandardJSON(testTable.toDF)
    assert(connector2.readStandardJSON() === expectedResult)
    connector2.deleteStandardJSON()
  }

}
