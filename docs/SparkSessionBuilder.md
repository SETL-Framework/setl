## Introduction

The class `SparkSessionBuilder` is used to configure and build new spark session for the given usage(s).

## Code Example

```scala
import com.jcdecaux.datacorp.spark.SparkSessionBuilder

// Auto-configure
val spark1: SparkSession = new SparkSessionBuilder("cassandra")
  .setAppName("myApp")
  .setEnv("dev")  // or AppEnv.DEV 
  .setCassandraHost("localhost")
  .build()
  .get()

// Build with your own SparkConf
val spark2: SparkSession = new SparkSessionBuilder()
  .configure(yourSparkConf)
  .build()
  .get()

```

