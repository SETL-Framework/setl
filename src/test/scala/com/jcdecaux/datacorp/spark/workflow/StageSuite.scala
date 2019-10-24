package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.storage.connector.CSVConnector
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class StageSuite extends FunSuite {

  import StageSuite._

  test("Stage Exceptions") {

    val stage = new Stage
    val fac = new MyFactoryStageTest

    assertThrows[AlreadyExistsException](stage.addFactory(fac).addFactory(fac))

  }

  test("Stage should not persist the output of a factory if persist of the factory is set to false") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val connectorOptions: Map[String, String] = Map[String, String](
      "path" -> "src/test/resources/test_csv_persistence",
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    )

    val connector = new CSVConnector(spark, connectorOptions)

    new Stage()
      .addFactory[PersistenceTest](Array(connector), false)
      .run()

    assertThrows[java.io.FileNotFoundException](connector.read(), "Output should not be persisted")
  }

  test("Stage should not persist the output of a factory if persist of the stage is set to false") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val connectorOptions: Map[String, String] = Map[String, String](
      "path" -> "src/test/resources/test_csv_persistence",
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    )

    val connector = new CSVConnector(spark, connectorOptions)
    val stage = new Stage().persist(false)

    stage
      .addFactory[PersistenceTest](Array(connector), persistence = true)
      .run()

    assertThrows[java.io.FileNotFoundException](connector.read(), "Output should not be persisted")
  }

  test("Stage should persist the output of a factory both persistence are set to true") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val connectorOptions: Map[String, String] = Map[String, String](
      "path" -> "src/test/resources/test_csv_persistence",
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    )

    val connector = new CSVConnector(spark, connectorOptions)
    val stage = new Stage().persist(true)

    stage
      .addFactory[PersistenceTest](Array(connector), persistence = true)
      .run()

    assert(connector.read().count() === 2, "Output should not be persisted")
    connector.delete()
  }
}

object StageSuite {

  case class PersistenceTestClass(col: String)

  class PersistenceTest(val connector: CSVConnector) extends Factory[Dataset[PersistenceTestClass]] {

    val spark: SparkSession = SparkSession.getActiveSession.get

    val output: Dataset[PersistenceTestClass] = spark.createDataset(Seq(
      PersistenceTestClass("a"),
      PersistenceTestClass("b")
    ))(ExpressionEncoder[PersistenceTestClass])

    override def read(): PersistenceTest.this.type = this

    override def process(): PersistenceTest.this.type = this

    override def write(): PersistenceTest.this.type = {
      this.connector.write(output.toDF())
      this
    }

    override def get(): Dataset[PersistenceTestClass] = output
  }

  class MyFactoryStageTest extends Factory[Container[Product23]] {

    var input: Container2[Product] = _
    var output: Container[Product23] = _

    @Delivery
    def setOutput(v: Container[Product23]): this.type = {
      output = v
      this
    }

    @Delivery
    def setInput(v: Container2[Product]): this.type = {
      input = v
      this
    }

    override def read(): MyFactoryStageTest.this.type = this

    override def process(): MyFactoryStageTest.this.type = this

    override def write(): MyFactoryStageTest.this.type = this

    override def get(): Container[Product23] = output
  }

}
