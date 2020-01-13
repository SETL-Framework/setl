# SETL

![AWS CodeBuild](https://codebuild.eu-west-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiWEJ3TXdNTUVsaUgveFE0V01xM1AvMkZaamVId1JFRGY0MDRuVWN1dEdmNkFKaFpmdmNlZXlTNFpGWjlXODFVQUNXdEIvc2tacXZuc3MySGpFU2gwVnk4PSIsIml2UGFyYW1ldGVyU3BlYyI6ImVqTmtldXdCWVlyd2JnMW0iLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.jcdecaux.setl/setl_2.11.svg?label=Maven%20Central&color=blue)](https://search.maven.org/search?q=g:%22com.jcdecaux.setl%22)
[![License](http://img.shields.io/:license-Apache%202-red.svg)](https://raw.githubusercontent.com/JCDecaux/setl/master/LICENSE)
[![Gitter](https://badges.gitter.im/setl-by-jcdecaux/community.svg)](https://gitter.im/setl-by-jcdecaux/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

SETL (Spark ETL, pronounced "settle") is an open-source Spark ETL framework that helps developers to structure ETL projects, modularize data transformation components and speed up the development.

## Use

### Create a new project
You can start working by cloning [this template project](https://github.com/qxzzxq/setl-template).

### Use in an existing project
```xml
<dependency>
  <groupId>com.jcdecaux.setl</groupId>
  <artifactId>setl_2.11</artifactId>
  <version>0.4.0</version>
</dependency>
```

To use the SNAPSHOT version, add Sonatype snapshot repository to your `pom.xml`
```xml
<repositories>
  <repository>
    <id>ossrh-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.jcdecaux.setl</groupId>
    <artifactId>setl_2.11</artifactId>
    <version>0.4.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

## Quick Start
### Basic concept
With SETL, an ETL application could be represented by a `Pipeline`. A `Pipeline` contains multiple `Stages`. In each stage, we could find one or several `Factories`.

The class `Factory[T]` is an abstraction of a data transformation that will produce an object of type `T`. It has 4 methods (*read*, *process*, *write* and *get*) that should be implemented by the developer.

The class `SparkRepository[T]` is a data access layer abstraction. It could be used to read/write a `Dataset[T]` from/to a datastore. It should be defined in a configuration file. You can have as many SparkRepositories as you want.

The entry point of a SETL project is the object `com.jcdecaux.setl.Setl`, which will handle the pipeline and spark repository instantiation.

### Show me some code
You can find the following tutorial code in [the starter template of SETL](https://github.com/qxzzxq/setl-template). Go and clone it :)

Here we show a simple example of creating and saving a **Dataset[TestObject]**. The case class **TestObject** is defined as follows:

```scala
case class TestObject(partition1: Int, partition2: String, clustering1: String, value: Long)
```

#### Context initialization
Suppose that we want to save our output into `src/main/resources/test_csv`. We can create a configuration file **local.conf** in `src/main/resources` with the following content that defines the target datastore to save our dataset:

```txt
testObjectRepository {
  storage = "CSV"
  path = "src/main/resources/test_csv"
  inferSchema = "true"
  delimiter = ";"
  header = "true"
  saveMode = "Append"
}
```

In our `App.scala` file, we build `Setl` and register this data store:
```scala  
val setl: Setl = Setl.builder()
  .withDefaultConfigLoader()
  .getOrCreate()

// Register a SparkRepository to context
setl.setSparkRepository[TestObject]("testObjectRepository")

```

#### Implementation of Factory
We will create our `Dataset[TestObject]` inside a `Factory[Dataset[TestObject]]`. A `Factory[A]` will always produce an object of type `A`, and it contains 4 abstract methods that you need to implement:
- read
- process
- write
- get

```scala
class MyFactory() extends Factory[Dataset[TestObject]] with HasSparkSession {

  // A repository is needed for writing data. It will be delivered by the pipeline
  @Delivery 
  private[this] val repo = SparkRepository[TestObject]

  private[this] var output: Dataset[TestObject] = _

  override def read(): MyFactory.this.type = {
    // we don't need to read any data
    this
  }

  override def process(): MyFactory.this.type = {
    import spark.implicits._
    output = Seq(
      TestObject(1, "a", "A", 1L),
      TestObject(2, "b", "B", 2L)
    ).toDS()
  this
  }

  override def write(): MyFactory.this.type = {
    repo.save(output)  // use the repository to save the output
    this
  }

  override def get(): Dataset[TestObject] = output

}
```

#### Define the pipeline
To execute the factory, we should add it into a pipeline.

We we call `setl.newPipeline()`, **Setl** will instantiate a new **Pipeline** and configure all the registered repositories as inputs of the pipeline. Then we call `addStage` to add our factory into the pipeline.

```scala
val pipeline = setl
  .newPipeline()
  .addStage[MyFactory]()
```

#### Run our pipeline
```scala
pipeline.describe().run()
```

The dataset will be saved into `src/main/resources/test_csv`

## Documentation
[Check our wiki](https://github.com/JCDecaux/setl/wiki)

## Contributing to SETL
[Check our contributing guide](https://github.com/JCDecaux/setl/blob/master/CONTRIBUTING.md)

