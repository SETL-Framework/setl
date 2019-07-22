package com.jcdecaux.datacorp.spark.storage

import java.io.File

//import java.util.TimeZone
//
//import com.datastax.spark.connector.cql.CassandraConnector
//import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
//import com.jcdecaux.datacorp.spark.enums.ValueType
//import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
//import org.apache.spark.sql.AnalysisException
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//class SparkRepositorySuite extends FunSuite with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {
//
//  import SparkRepositorySuite.deleteRecursively
//
//  var mockCassandra: MockCassandra = _
//
//  val testTable: Seq[TestObject] = Seq(
//    TestObject(1, "p1", "c1", 1L),
//    TestObject(2, "p2", "c2", 2L),
//    TestObject(3, "p3", "c3", 3L)
//  )
//
//  override def clearCache(): Unit = CassandraConnector.evictCache()
//
//  //Sets up CassandraConfig and SparkContext
//  System.setProperty("test.cassandra.version", "3.11.4")
//  useCassandraConfig(Seq(YamlTransformations.Default))
//  useSparkConf(defaultConf)
//  val connector = CassandraConnector(defaultConf)
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    mockCassandra = new MockCassandra(connector, MockCassandra.keyspace)
//      .generateKeyspace()
//
//
//  }
//
//  test("Cassandra") {
//    val spark = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()
//    import spark.implicits._
//
//    // Test read and write
//    val cqlRepo = new TestObjectCassandraSparkRepository(spark)
//    assertThrows[IOException](cqlRepo.findAll())
//    cqlRepo.save(testTable.toDS())
//    assert(cqlRepo.findAll().count() === 3)
//
//    // Test filter
//    val filter = Set(
//      Filter("partition1", "=", "int", Some("1")),
//      Filter("partition2", "=", "string", Some("p1"))
//    )
//    assert(cqlRepo.findBy(filter).count() === 1)
//
//  }
//
//  test("find by date") {
//    mockCassandra.generateDwellTime("dwell")
//    val spark = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()
//    import spark.implicits._
//    /*
//     * find by date
//     */
//    val dwellRepo = new TestDwellTimeRepository(spark)
//    assert(dwellRepo.findAll().count() === 39)
//
//    val dateFilter = Set(Condition("date", ">", Some("2018-01-01"), ValueType.DATE))
//    assert(dwellRepo.findByCondition(dateFilter).count() === 11)
//
//    val dateFilter2 = Set(
//      Condition("date", ">", Some("2018-01-01"), ValueType.DATE),
//      Condition("date", "<=", Some("2018-01-02"), ValueType.DATE)
//    )
//    assert(dwellRepo.findByCondition(dateFilter2).count() === 7)
//
//    val dateFilter3 = Condition("date", "=", Some("2018-01-03"), ValueType.DATE)
//    assert(dwellRepo.findByCondition(dateFilter3).count() === 4)
//  }
//
//  test("find by datetime") {
//    mockCassandra.generateTraffic("traffic")
//
//    val spark = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()
//    import spark.implicits._
//    /*
//     * find by datetime
//     */
//    val trafficRepo = new TestTrafficRepository(spark)
//    assert(trafficRepo.findAll().count() === 24 * 4)
//
//    val datetimeCondition = Set(
//      Condition("datetime", ">=", Some("2018-01-01 00:00:00"), ValueType.DATETIME),
//      Condition("datetime", "<=", Some("2018-01-01 05:00:00"), ValueType.DATETIME)
//    )
//    println(TimeZone.getDefault)
//    trafficRepo.findByCondition(datetimeCondition).show(200)
//    assert(trafficRepo.findByCondition(datetimeCondition).count() === 24, "Please make sure that the system timezone is set to UTC")
//
//    val datetimeCondition2 = Set(
//      Condition("asset", "=", Some("asset-3"), ValueType.STRING),
//      Condition("datetime", ">", Some("2018-01-01 17:00:00"), ValueType.DATETIME)
//    )
//    assert(trafficRepo.findByCondition(datetimeCondition2).count() === 6)
//
//    val datetimeCondition3 = Set(
//      Condition("asset", "=", Some("asset-3"), ValueType.STRING),
//      Condition("datetime", "=", Some("2018-01-01 17:00:00"), ValueType.DATETIME)
//    )
//    assert(trafficRepo.findByCondition(datetimeCondition3).count() === 1)
//
//  }
//
//  test("CSV") {
//    val spark = new SparkSessionBuilder().setEnv("dev").build().get()
//    import spark.implicits._
//
//    // Test read and write
//    val csvRepo = new TestObjectCsvSparkRepository(spark)
//    assertThrows[AnalysisException](csvRepo.findAll())
//
//    csvRepo.save(testTable.toDS())
//    assert(csvRepo.findAll().count() === 3)
//
//    // Test filter
//    val filter = Set(
//      Filter("partition1", "=", "int", Some("1")),
//      Filter("partition2", "=", "string", Some("p1"))
//    )
//    assert(csvRepo.findBy(filter).count() === 1)
//
//    deleteRecursively(new File(csvRepo.path))
//  }
//
//  test("Parquet") {
//    val spark = new SparkSessionBuilder().setEnv("dev").build().get()
//    import spark.implicits._
//
//    // Test read and write
//    val parquetRepo = new TestObjectParquetSparkRepository(spark)
//    assertThrows[AnalysisException](parquetRepo.findAll())
//
//    parquetRepo.save(testTable.toDS())
//    assert(parquetRepo.findAll().count() === 3)
//
//    // Test filter
//    val filter = Set(
//      Filter("partition1", "=", "int", Some("1")),
//      Filter("partition2", "=", "string", Some("p1"))
//    )
//    assert(parquetRepo.findBy(filter).count() === 1)
//
//    deleteRecursively(new File(parquetRepo.path))
//  }
//
//}
//
object SparkRepositorySuite {
  def deleteRecursively(file: File): Unit = {
    println(s"Remove ${file.getName}")
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}