package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.annotation.Delivery
import com.jcdecaux.setl.exception.AlreadyExistsException
import com.jcdecaux.setl.storage.connector.CSVConnector
import com.jcdecaux.setl.transformation.Factory
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

class StageSuite extends AnyFunSuite {

  import StageSuite._

  override def withFixture(test: NoArgTest): Outcome = {
    // Shared setup (run at beginning of each test)
    SparkSession.getActiveSession match {
      case Some(ss) => ss.stop()
      case _ =>
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    try test()
    finally {
      // Shared cleanup (run at end of each test)
      SparkSession.getActiveSession match {
        case Some(ss) => ss.stop()
        case _ =>
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

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

    val connector = new CSVConnector(connectorOptions)

    new Stage()
      .addFactory[PersistenceTest](Array(connector), writable = false)
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

    val connector = new CSVConnector(connectorOptions)
    val stage = new Stage().writable(false)

    stage
      .addFactory[PersistenceTest](Array(connector), writable = true)
      .run()

    assertThrows[java.io.FileNotFoundException](connector.read(), "Output should not be persisted")
  }

  test("Stage should persist the output of a factory both writable are set to true") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()

    val connectorOptions: Map[String, String] = Map[String, String](
      "path" -> "src/test/resources/test_csv_persistence",
      "inferSchema" -> "true",
      "delimiter" -> "|",
      "header" -> "true",
      "saveMode" -> "Append"
    )

    val connector = new CSVConnector(connectorOptions)
    val stage = new Stage().writable(true)

    stage
      .addFactory[PersistenceTest](Array(connector), writable = true)
      .run()

    assert(connector.read().count() === 2, "Output should be persisted")
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

    private[this] def writeData(): Unit = this.connector.write(output.toDF())

    override def write(): PersistenceTest.this.type = {

      /*
      Mysterious error caused by Spark, wait two sec before retry
      java.lang.IllegalArgumentException: Error while instantiating 'org.apache.spark.sql.internal.SessionStateBuilder':
      ...
      Cause: java.lang.IllegalStateException: RpcEnv has been stopped
       */

      try {
        writeData()
      } catch {
        case _: Throwable =>
          val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
          Thread.sleep(2000)
          writeData()
      }

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
