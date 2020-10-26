**StructuredStreamingConnector** is a new connector added since the version 0.4.3. It brings the Spark Structured Streaming API together with the Connector API. It allows users to manipulate streaming data like any other static connectors.
 
Here is an implementation of the [word count program](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example) from the Spark structured streaming documentation:

```scala
  // Configuration
  val input = Map(
    "storage" -> "STRUCTURED_STREAMING",
    "format" -> "socket",
    "host" -> "localhost",
    "port" -> "9999"
  )

  val output = Map(
    "storage" -> "STRUCTURED_STREAMING",
    "outputMode" -> "complete",
    "format" -> "console"
  )

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val inputConnector = new ConnectorBuilder(Conf.fromMap(input)).getOrCreate()
  val outputConnector = new ConnectorBuilder(Conf.fromMap(output)).getOrCreate().asInstanceOf[StructuredStreamingConnector]
  
  // read lines
  val lines = inputConnector.read()
  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))
  // Generate running word count
  val wordCounts = words.groupBy("value").count()
  // Show the output
  outputConnector.write(wordCounts)
  outputConnector.awaitTermination()
```