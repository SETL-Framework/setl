package io.github.setl.storage.connector

import io.github.setl.config.{HudiConnectorConf, Properties}
import io.github.setl.{SparkSessionBuilder, SparkTestUtils, TestObject2}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths
import java.sql.{Date, Timestamp}

class HudiConnectorSuite extends AnyFunSuite {

  val path: String = Paths.get("src", "test", "resources", "test_hudi").toFile.getAbsolutePath
  val saveMode = SaveMode.Overwrite

  val options: Map[String, String] = Map[String, String](
    "path" -> path,
    "saveMode" -> saveMode.toString,
    "hoodie.table.name" -> "test_object",
    "hoodie.datasource.write.recordkey.field" -> "col1",
    "hoodie.datasource.write.precombine.field" -> "col4",
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
    "hoodie.index.type" -> "BUCKET",
    "hoodie.bucket.index.num.buckets" -> "1",
    "hoodie.storage.layout.partitioner.class" -> "org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner"
  )

  val testTable: Seq[TestObject2] = Seq(
    TestObject2("string", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string2", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L),
    TestObject2("string3", 5, 0.000000001685400132103450D, new Timestamp(1557153268000L), new Date(1557100800000L), 999999999999999999L)
  )

  test("Instantiation of constructors") {

    // New spark session here since Hudi only supports KryoSerializer
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .build()
      .get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    import spark.implicits._

    val connector = new HudiConnector(HudiConnectorConf.fromMap(options))
    connector.write(testTable.toDF)
    assert(connector.read().collect().length == testTable.length)

    val connector7 = new HudiConnector(Properties.hudiConfig)
    connector7.write(testTable.toDF)
    assert(connector7.read().collect().length == testTable.length)
  }
}
