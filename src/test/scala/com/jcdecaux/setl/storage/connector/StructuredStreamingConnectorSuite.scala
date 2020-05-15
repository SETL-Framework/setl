package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.SparkSessionBuilder
import com.jcdecaux.setl.config.Conf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class StructuredStreamingConnectorSuite extends AnyFunSuite {

  val inputConf: Map[String, String] = Map(
    "format" -> "text",
    "path" -> "src/test/resources/streaming_test_resources/input"
  )

  val consoleOutputConf: Map[String, String] = Map(
    "format" -> "console",
    "outputMode" -> "append"
  )

  val parquetOutputConf: Map[String, String] = Map(
    "format" -> "PARQUET",
    "outputMode" -> "append",
    "checkpointLocation" -> "src/test/resources/streaming_test_resources/output/checkpoint_1",
    "path" -> "src/test/resources/streaming_test_resources/output/1"
  )

  test("StructuredStreamingConnector instantiation") {
    val spark: SparkSession = new SparkSessionBuilder().setEnv("local").build().get()
    import spark.implicits._

    val _conf = Conf.fromMap(parquetOutputConf)

    val connector = new StructuredStreamingConnector(inputConf)
    val outputConnector = new StructuredStreamingConnector(_conf)
    val parquetConnector = new ParquetConnector(parquetOutputConf)

    val input = connector.read()

    outputConnector.write(input, Option("suffix_should_be_ignored"))
    outputConnector.awaitTerminationOrTimeout(10000)

    parquetConnector.read().show()
    assert(parquetConnector.read().as[String].collect().mkString(" ") === StructuredStreamingConnectorSuite.text)
  }

}

object StructuredStreamingConnectorSuite {
  val text = "Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs. In short, Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming. Internally, by default, Structured Streaming queries are processed using a micro-batch processing engine, which processes data streams as a series of small batch jobs thereby achieving end-to-end latencies as low as 100 milliseconds and exactly-once fault-tolerance guarantees. However, since Spark 2.3, we have introduced a new low-latency processing mode called Continuous Processing, which can achieve end-to-end latencies as low as 1 millisecond with at-least-once guarantees. Without changing the Dataset/DataFrame operations in your queries, you will be able to choose the mode based on your application requirements. In this guide, we are going to walk you through the programming model and the APIs. We are going to explain the concepts mostly using the default micro-batch processing model, and then later discuss Continuous Processing model. First, let’s start with a simple example of a Structured Streaming query - a streaming word count."
}
