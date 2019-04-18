package com.jcdecaux.datacorp.spark.storage

import java.io.{File, IOException}

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import com.jcdecaux.datacorp.spark.{MockCassandra, SparkSessionBuilder, TestObject}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkRepositorySuite extends FunSuite with EmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {

  import SparkRepositorySuite.deleteRecursively

  val testTable: Seq[TestObject] = Seq(
    TestObject(1, "p1", "c1", 1L),
    TestObject(2, "p2", "c2", 2L),
    TestObject(3, "p3", "c3", 3L)
  )

  override def clearCache(): Unit = CassandraConnector.evictCache()

  //Sets up CassandraConfig and SparkContext
  System.setProperty("test.cassandra.version", "3.11.4")
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)
  val connector = CassandraConnector(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    new MockCassandra(connector, MockCassandra.keyspace)
      .generateKeyspace()
  }

  test("Cassandra") {
    val spark = new SparkSessionBuilder("cassandra").setEnv("dev").setCassandraHost("localhost").build().get()
    import spark.implicits._

    // Test read and write
    val cqlRepo = new TestObjectCassandraSparkRepository(spark)
    assertThrows[IOException](cqlRepo.findAll())
    cqlRepo.save(testTable.toDS())
    assert(cqlRepo.findAll().count() === 3)

    // Test filter
    val filter = Set(
      Filter("partition1", "=", "int", Some("1")),
      Filter("partition2", "=", "string", Some("p1"))
    )
    assert(cqlRepo.findBy(filter).count() === 1)
  }

  test("CSV") {
    val spark = new SparkSessionBuilder().setEnv("dev").build().get()
    import spark.implicits._

    // Test read and write
    val csvRepo = new TestObjectCsvSparkRepository(spark)
    assertThrows[AnalysisException](csvRepo.findAll())

    csvRepo.save(testTable.toDS())
    assert(csvRepo.findAll().count() === 3)

    // Test filter
    val filter = Set(
      Filter("partition1", "=", "int", Some("1")),
      Filter("partition2", "=", "string", Some("p1"))
    )
    assert(csvRepo.findBy(filter).count() === 1)

    deleteRecursively(new File(csvRepo.path))
  }

  test("Parquet") {
    val spark = new SparkSessionBuilder().setEnv("dev").build().get()
    import spark.implicits._

    // Test read and write
    val parquetRepo = new TestObjectParquetSparkRepository(spark)
    assertThrows[AnalysisException](parquetRepo.findAll())

    parquetRepo.save(testTable.toDS())
    assert(parquetRepo.findAll().count() === 3)

    // Test filter
    val filter = Set(
      Filter("partition1", "=", "int", Some("1")),
      Filter("partition2", "=", "string", Some("p1"))
    )
    assert(parquetRepo.findBy(filter).count() === 1)

    deleteRecursively(new File(parquetRepo.path))
  }

}

object SparkRepositorySuite {
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}