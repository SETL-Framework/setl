package com.jcdecaux.setl.storage.repository

import java.io.{ByteArrayOutputStream, File}

import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.internal.TestClasses.InnerClass
import com.jcdecaux.setl.storage.Condition
import com.jcdecaux.setl.storage.connector._
import com.jcdecaux.setl.{SparkSessionBuilder, SparkTestUtils, TestObject}
import org.apache.log4j.{Logger, SimpleLayout, WriterAppender}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkRepositorySuite extends AnyFunSuite with Matchers {

  import com.jcdecaux.setl.util.IOUtils.deleteRecursively

  val path: String = "src/test/resources/test_parquet"
  val table: String = "test_table"

  val options: Map[String, String] = Map[String, String](
    "inferSchema" -> "false",
    "delimiter" -> ";",
    "header" -> "true"
  )

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  val innerClass = Seq(
    InnerClass("i1", "你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见"),
    InnerClass("i11", "165498465DDDFKLJKSDOIJ__çezé*/-+165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé165498465DDDFKLJKSDOIJ__çezé")
  )

  val testCompressionRepository = Seq(
    TestCompressionRepository("col1_1", "col2", innerClass, Seq("谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见", "b", "c谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见")),
    TestCompressionRepository("col1_2", "col2", innerClass, Seq("a", "谢再见你好谢谢再见你好谢谢qsdfqsdfqsdfqsdfqsdf sqdfsdqf qs 再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见", "c")),
    TestCompressionRepository("col1_3", "col2", innerClass, Seq("谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见a", "b", "c"))
  )

  val testCompressionRepositoryGZIP = Seq(
    TestCompressionRepositoryGZIP("col1_1", "col2", innerClass, Seq("谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见", "b", "c谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见")),
    TestCompressionRepositoryGZIP("col1_2", "col2", innerClass, Seq("a", "谢再见你好谢谢再见你好谢谢qsdfqsdfqsdfqsdfqsdf sqdfsdqf qs 再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见", "c")),
    TestCompressionRepositoryGZIP("col1_3", "col2", innerClass, Seq("谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见你好谢谢再见a", "b", "c"))
  )


  test("Instantiation") {
    assertThrows[SparkException](new SparkRepository().spark)

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val parquetConnector = new ParquetConnector(path, SaveMode.Overwrite)
    import spark.implicits._

    val repo = new SparkRepository[TestObject]().setConnector(parquetConnector)
    val condition = Condition("partition1", "=", 1L)

    assert(repo.getStorage === Storage.PARQUET)
    repo.save(testTable.toDS())
    repo.save(testTable.toDS())
    val test = repo.findAll()
    assert(test.count() === 3)
    assert(repo.getConnector === parquetConnector)

    assert(repo.findBy(condition).count() === 1)
    deleteRecursively(new File(path))

  }

  test("SparkRepository should handle case class with CompoundKey annotation") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
    val path: String = "src/test/resources/test_parquet_with_anno"
    val connector = new CSVConnector(Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))
    val condition = Condition("col1", "=", "a")

    val repo = new SparkRepository[MyObject].setConnector(connector)

    repo
      .partitionBy("_sort_key")
      .save(ds)

    // to verify if the filter is pushed down
    val filteredBySortKey = repo.findBy(Condition("_sort_key", "=", "A-a"))
    filteredBySortKey.explain()
    assert(filteredBySortKey.count() === 1)

    val data = repo.findAll()
    assert(data.columns === Array("column1", "column2"))

    val rawData = connector.read()
    rawData.show()
    assert(rawData.columns === Array("col1", "column2", "_sort_key"))

    val filteredData = repo.findBy(condition)
    assert(filteredData.columns === Array("column1", "column2"))
    assert(filteredData.count() === 1)

    deleteRecursively(new File(path))

    val url: String = s"jdbc:postgresql://${JDBCConnectorSuite.psqlHost}:5432/framework_dev"
    val user: String = "postgres"
    val password: String = "postgres"

    val options: Map[String, String] = Map(
      "url" -> url,
      "dbtable" -> "unittest",
      "saveMode" -> "Overwrite",
      "user" -> user,
      "password" -> password
    )
    val connector2 = new JDBCConnector(options)
    connector2.write(ds.toDF())
    val repo2 = new SparkRepository[MyObject].setConnector(connector2)
    repo2.partitionBy("partition")
    assert(!connector2.read().columns.contains("partition"))
  }

  test("SparkRepository should handle update (upsert) when ACIDConnector is used") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4.2"))

    import spark.implicits._

    val ds: Dataset[TestDeltaUpdate] = Seq(TestDeltaUpdate(1, "c1", 2.3D), TestDeltaUpdate(2, "c2", 4.9D)).toDS()
    val dss: Dataset[TestDeltaUpdate] = Seq(TestDeltaUpdate(1, "c1bis", 4.4D), TestDeltaUpdate(1, "c1", 5.1D)).toDS()
    val path: String = "src/test/resources/test_delta_with_anno"
    val connector = new DeltaConnector(Map[String, String](
      "path" -> path,
      "saveMode" -> "Append"
    ))
    val condition = Condition("col1", "=", 2)

    val repo = new SparkRepository[TestDeltaUpdate].setConnector(connector)

    connector.drop()

    repo
      .partitionBy("_sort_key")
      .save(ds)

    val filteredBySortKey = repo.findBy(Condition("_sort_key", "=", "c2"))
    assert(filteredBySortKey.count() === 1)

    val data = repo.findAll()
    assert(data.columns === Array("column1", "column2", "value"))

    val rawData = connector.read()
    rawData.show()
    assert(rawData.columns === Array("col1", "column2", "value", "_partition_key", "_sort_key"))

    val filteredData = repo.findBy(condition)
    assert(filteredData.columns === Array("column1", "column2", "value"))
    assert(filteredData.count() === 1)

    repo
      .partitionBy("_sort_key")
      .update(dss)

    val rawDataAfterUpdate = connector.read()

    assert(rawDataAfterUpdate.count() === 3)

    val filteredDataAfterUpdate = repo.findBy(condition)
    assert(filteredDataAfterUpdate.count() === 1)

    connector.drop()
  }

  test("SparkRepository should handle UDS key configuration") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val path: String = "src/test/resources/test_parquet_with_anno"
    val connector = new CSVConnector(Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    )).asInstanceOf[Connector]
    val repo = new SparkRepository[MyObject].setConnector(connector)

    assert(connector.asInstanceOf[CSVConnector].getUserDefinedSuffixKey === "_user_defined_suffix")
    assert(repo.getUserDefinedSuffixKey === Some("_user_defined_suffix"))
    repo.setUserDefinedSuffixKey("TestKey")
    assert(connector.asInstanceOf[CSVConnector].getUserDefinedSuffixKey === "TestKey")
    assert(repo.getUserDefinedSuffixKey === Some("TestKey"))

    val logger = Logger.getLogger(classOf[SparkRepository[MyObject]])
    val outContent = new ByteArrayOutputStream()
    val appender = new WriterAppender(new SimpleLayout, outContent)
    logger.addAppender(appender)
    val warnMessage = "Current connector doesn't support user defined suffix, skip UDS setting"
    val connector2 = new JDBCConnector(Map(
      "url" -> "url",
      "dbtable" -> "unittest",
      "saveMode" -> "Overwrite",
      "user" -> "user",
      "password" -> "password"
    ))
    val repo2 = new SparkRepository[MyObject].setConnector(connector2)
    assert(repo2.getUserDefinedSuffixKey === None)
    repo2.setUserDefinedSuffixKey("key")
    assert(outContent.toString.contains(warnMessage))
    assert(repo2.getUserDefinedSuffixKey === None)
  }

  test("Test spark repository save with suffix") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
    val path: String = "src/test/resources/test_parquet_with_annotation"
    val connector = new CSVConnector(Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))

    val repo = new SparkRepository[MyObject].setConnector(connector)
    val repo2 = new SparkRepository[MyObject].setConnector(null)
    assertThrows[NullPointerException](repo2.save(ds))

    repo.save(ds, Some("1"))
    repo.save(ds, Some("2"))
    repo.save(ds, Some("3"))
    repo.save(ds, Some("3/4/5"))

    val df = repo.findAll()
    assert(df.count() === 8)

    df.show()
    connector.delete()
  }

  test("SparkRepository should handle column name changed by annotation while filtering") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").setSparkMaster("local").build().get()
    import spark.implicits._

    val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B"), MyObject("b", "BB"), MyObject("b", "BBB")).toDS()
    val path: String = "src/test/resources/test_spark_repository_colname_change"
    val connector = new CSVConnector(Map[String, String](
      "path" -> path,
      "inferSchema" -> "false",
      "delimiter" -> ";",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    ))

    val condition = Condition("col1", "=", "a")
    val condition2 = Condition("column1", "=", "a")
    val condition3 = Condition($"col1" === "a")
    val condition4 = Condition($"column1" === "a")
    val repo = new SparkRepository[MyObject].setConnector(connector)
    repo.save(ds)

    val finded1 = repo.findBy(condition).collect()
    val finded2 = repo.findBy(condition2).collect()
    val finded3 = repo.findBy(condition3).collect()
    val finded4 = repo.findBy(condition4).collect()
    val finded5 = repo.findBy($"column1" === "a").collect()
    val finded6 = repo.findBy($"col1" === "a").collect()

    finded1 should contain theSameElementsAs finded2
    finded2 should contain theSameElementsAs finded3
    finded3 should contain theSameElementsAs finded4
    finded5 should contain theSameElementsAs finded4
    finded5 should contain theSameElementsAs finded6

    val conditionBis = Set(Condition("col1", "=", "b"), Condition("_sort_key", "IN", Set("BBB-b", "BB-b")))
    val conditionBis2 = Condition($"col1" === "b" && $"_sort_key".isin("BBB-b", "BB-b"))
    val conditionBis3 = Set(Condition("col1", "=", "b"), Condition($"_sort_key".isin("BBB-b", "BB-b")))

    val findedBis1 = repo.findBy(conditionBis)
    val findedBis2 = repo.findBy(conditionBis2)
    val findedBis3 = repo.findBy(conditionBis3)
    val findedBis4 = repo.findBy($"col1" === "b" && $"_sort_key".isin("BBB-b", "BB-b"))
    val findedBis5 = repo.findBy($"column1" === "b" && $"_sort_key".isin("BBB-b", "BB-b"))

    findedBis1.collect() should contain theSameElementsAs findedBis2.collect()
    findedBis2.collect() should contain theSameElementsAs findedBis3.collect()
    findedBis4.collect() should contain theSameElementsAs findedBis3.collect()
    findedBis4.collect() should contain theSameElementsAs findedBis5.collect()

    connector.delete()
  }

  test("SparkRepository should compress columns with Compress annotation") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    import spark.implicits._

    val test = spark.createDataset(testCompressionRepository)

    val path: String = "src/test/resources/test_spark_repository_compression"
    val connector = new ParquetConnector(Map[String, String](
      "path" -> path,
      "table" -> "test_repo_compression",
      "saveMode" -> "Append"
    ))

    val repo = new SparkRepository[TestCompressionRepository].setConnector(connector)

    // Write non compressed data
    connector.write(test.toDF())
    val sizeUncompressed = connector.getSize
    connector.delete()

    // Write compressed data
    repo.save(test)
    val sizeCompressed = connector.getSize

    println(s"size before compression: $sizeUncompressed")
    println(s"size after compression: $sizeCompressed")
    assert(sizeCompressed <= sizeUncompressed)

    val loadedData = repo.findAll()
    loadedData.show()
    loadedData.collect() should contain theSameElementsAs test.collect()
    assert(test.filter(_.col1 == "col1_3").head === repo.findBy(Condition("col1", "=", "col1_3")).head)

    // Exception will be thrown when we try to filter a binary column
    assertThrows[IllegalArgumentException](repo.findBy(Condition("col4", "=", "test")))
    assertThrows[IllegalArgumentException](repo.findBy(Set(Condition("col1", "=", "haha"), Condition("col4", "=", "test"))))
    assertThrows[IllegalArgumentException](repo.findBy($"col4" === "test"))
    assertThrows[IllegalArgumentException](repo.findBy($"col4" === "test" && $"col1" === "haha"))
    connector.delete()
  }

  test("SparkRepository should compress columns with Compress annotation with another Compressor") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    assume(SparkTestUtils.checkSparkVersion("2.4"))

    import spark.implicits._

    val test = spark.createDataset(testCompressionRepositoryGZIP)

    val path: String = "src/test/resources/test_spark_repository_compression_gzip"
    val connector = new ParquetConnector(Map[String, String](
      "path" -> path,
      "table" -> "test_repo_compression_gzip",
      "saveMode" -> "Overwrite"
    ))

    val repo = new SparkRepository[TestCompressionRepositoryGZIP].setConnector(connector)

    // Write non compressed data
    connector.write(test.toDF())
    val sizeUncompressed = connector.getSize
    connector.delete()

    // Write compressed data
    repo.save(test)
    val sizeCompressed = connector.getSize

    println(s"size before compression: $sizeUncompressed")
    println(s"size after compression: $sizeCompressed")
    assert(sizeCompressed <= sizeUncompressed)

    val loadedData = repo.findAll()
    loadedData.show()
    loadedData.collect() should contain theSameElementsAs test.collect()
    assert(test.filter(_.col1 == "col1_3").head === repo.findBy(Condition("col1", "=", "col1_3")).head)

    // Exception will be thrown when we try to filter a binary column
    assertThrows[IllegalArgumentException](repo.findBy(Condition("col4", "=", "test")))
    assertThrows[IllegalArgumentException](repo.findBy(Set(Condition("col1", "=", "haha"), Condition("col4", "=", "test"))))
    assertThrows[IllegalArgumentException](repo.findBy($"col4" === "test"))
    assertThrows[IllegalArgumentException](repo.findBy($"col4" === "test" && $"col1" === "haha"))
    connector.delete()
  }

  test("SparkRepository should cache read data unless there are new data be written") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    assume(SparkTestUtils.checkSparkVersion("2.4"))

    import spark.implicits._

    val testData = spark.createDataset(testCompressionRepositoryGZIP)

    val path: String = "src/test/resources/test_spark_repository_read_cache"
    val connector = new ParquetConnector(Map[String, String](
      "path" -> path,
      "table" -> "test_repo_compression",
      "saveMode" -> "Append"
    ))

    // Test cache-enabled repository
    val repoCached = new SparkRepository[TestCompressionRepositoryGZIP].setConnector(connector).persistReadData(true)
    assert(repoCached.persistReadData)
    assert(repoCached.getReadCacheStorageLevel.useMemory)
    repoCached.save(testData)
    val r1 = repoCached.findAll()

    val field = repoCached.getClass.getDeclaredField("readCache")
    field.setAccessible(true)
    assert(field.get(repoCached).asInstanceOf[DataFrame].storageLevel.useMemory)
    connector.delete()

    // Test cache-disabled repository
    val repoNotCached = new SparkRepository[TestCompressionRepositoryGZIP].setConnector(connector)
    assert(!repoNotCached.persistReadData)
    assert(repoNotCached.getReadCacheStorageLevel.useMemory)
    repoNotCached.save(testData)
    val r2 = repoNotCached.findAll()

    val field2 = repoCached.getClass.getDeclaredField("readCache")
    field2.setAccessible(true)
    assert(!field2.get(repoNotCached).asInstanceOf[DataFrame].storageLevel.useMemory)
    connector.delete()
  }

  test("") {

  }
}
