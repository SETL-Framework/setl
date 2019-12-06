package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.storage.Condition
import com.jcdecaux.datacorp.spark.storage.connector.CSVConnector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class RepositoryAdapterSuite extends FunSuite {

  val path: String = "src/test/resources/test_repository_adapter"

  val data: Seq[RepoAdapterTesterA] = Seq(
    RepoAdapterTesterA("a", "A"),
    RepoAdapterTesterA("b", "B")
  )

  test("RepositoryAdapter should implicitly convert two dataset") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val ds: Dataset[RepoAdapterTesterA] = spark.createDataset(data)(ExpressionEncoder[RepoAdapterTesterA])

    import com.jcdecaux.datacorp.spark.storage.repository.ImplicitConverter.a2b
    import com.jcdecaux.datacorp.spark.storage.repository.ImplicitRepositoryAdapter._

    val options: Map[String, String] = Map[String, String](
      "path" -> path,
      "inferSchema" -> "true",
      "delimiter" -> ",",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    )

    val csvConnector = new CSVConnector(options)

    val repo: SparkRepository[RepoAdapterTesterA] =
      new SparkRepository[RepoAdapterTesterA]().setConnector(csvConnector)

    repo.convertAndSave(ds)
    val ds2 = repo.findAllAndConvert()
    val df = csvConnector.read()

    assert(ds2.columns === ds.columns)
    assert(df.columns === Array("column1", "col2", "col3"))
    csvConnector.delete()
  }

  test("RepositoryAdapter should be able to handle filter") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    val ds: Dataset[RepoAdapterTesterA] = spark.createDataset(data)(ExpressionEncoder[RepoAdapterTesterA])

    import com.jcdecaux.datacorp.spark.storage.repository.ImplicitConverter.a2b
    import com.jcdecaux.datacorp.spark.storage.repository.ImplicitRepositoryAdapter._

    val options: Map[String, String] = Map[String, String](
      "path" -> (path + "_filter"),
      "inferSchema" -> "true",
      "delimiter" -> ",",
      "header" -> "true",
      "saveMode" -> "Overwrite"
    )

    val csvConnector = new CSVConnector(options)

    val repo: SparkRepository[RepoAdapterTesterA] =
      new SparkRepository[RepoAdapterTesterA]().setConnector(csvConnector)

    repo.convertAndSave(ds)

    val conditions = Condition("column1", "=", "a")

    assert(repo.findByAndConvert(conditions).count() === 1)
    csvConnector.delete()
  }

}
