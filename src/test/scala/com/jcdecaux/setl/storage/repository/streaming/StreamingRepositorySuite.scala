package com.jcdecaux.setl.storage.repository.streaming

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.exception.InvalidConnectorException
import com.jcdecaux.setl.storage.connector.CSVConnector
import com.jcdecaux.setl.storage.repository.SparkRepository
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class StreamingRepositorySuite extends AnyFunSuite {

  test("StreamingRepository should throw exception") {
    import com.jcdecaux.setl.storage.repository.streaming.StreamingRepositorySuite.TestClass

    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").getOrCreate()

    val csvOutputConf: Map[String, String] = Map(
      "path" -> "src/test/resources/streaming_test_resources/output/3",
      "header" -> "true"
    )
    val csvConnector = new CSVConnector(csvOutputConf)
    val repo = new SparkRepository[TestClass]().setConnector(csvConnector)

    assertThrows[InvalidConnectorException](repo.awaitTermination())
    assertThrows[InvalidConnectorException](repo.awaitTerminationOrTimeout(1))
  }

}

object StreamingRepositorySuite {

  case class TestClass(x: String)

}
