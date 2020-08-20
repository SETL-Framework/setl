# Definition
**Setl** is the entry point of the framework. It handles the pipeline instantiation and the management of repositories and connectors.

# Instantiation

Instantiation with default configuration:
```scala
val setl = Setl.builder()
  .withDefaultConfigLoader()  // load the default config file according to the jvm property: app.environment
  .getOrCreate()
```

Or you can specify a configuration file:
```scala
val setl = Setl.builder()
  .withDefaultConfigLoader("conf_file_name.conf") 
  .getOrCreate()
```

Or you can use your own **SparkConf** object:
```scala
val setl = Setl.builder()
  .setSparkConf(sparkConf)
  .withDefaultConfigLoader() 
  .getOrCreate()
```

Or you can specify the config path of Setl settings in your configuration file:
```scala
val setl = Setl.builder()
  .setSetlConfigPath("path_of_setl_configuration")
  .withDefaultConfigLoader() 
  .getOrCreate()
```

# Repository management
**Setl** helps you to create and manage the [**SparkRepository**](Repository) of your application.

To register spark repository:
```scala
setl
  .setSparkRepository[MyClass]("configPath")
  .setSparkRepository[MyClass]("anotherConfigPath", consumer = Seq[Class[MyFactory1], Class[MyFactory2])
```

When Setl creates a new pipeline, all the registered SparkRepositories will be passed into the delivery pool.

To get a spark repository:
```scala
val myClassRepository: SparkRepository[MyClass] = setl.getSparkRepository[MyClass]("configPath")
```

You can have as many SparkRepositories as possible, even if they have the same type. Each registered spark repository is identified by its config path.

:warning: the `setSparkRepository` method will not update an existing repository having the same config path. To update a registered repository, use the `resetSparkRepository` method.

To use a repository in a **Factory**
```scala
class MyFactory extends Factory[Any] {
  @Delivery
  var repo: SparkRepository[MyClass] = _  // this variable will be delivered at runtime

  @Delivery(autoLoad = true)
  var ds: Dataset[MyClass] = _  // pipeline will invoke the "findAll" method of an available  
                                // SparkRepository[MyClass], and deliver the result to this variable.
}
```

# Connector management
**Setl** handles [**Connectors**](Connector) in a similar way as it handles repositories. You can have as many connectors as possible as long as each of them has a different configuration path and delivery ID.

To register a connector:
```scala
setl
  .setConnector("config_path_1")  // type: Connector, deliveryID: config_path_1
  .setConnector("config_path_2", "id1")  // type: Connector, deliveryID: id1
  .setConnector("config_path_3", classOf[CSVConnector])  // type: CSVConnector, deliveryID: config_path_3
  .setConnector("config_path_4", "id2", classOf[DynamoDBConnector])  // type: DynamoDBConnector, deliveryID: id2
```

To use connectors in a **Factory**
```scala
class MyFactory extends Factory[Any] {
  @Delivery(id = "config_path_1")
  var connector1: Connector = _ 

  @Delivery(id = "id1")
  var connector2: Connector = _ 

  @Delivery(id = "config_path_3")
  var connector3: CSVConnector = _ 

  @Delivery(id = "id2")
  var connector4: DynamoDBConnector = _ 
}
```

# Pipeline management
A new [**Pipeline**](Pipeline) could be instantiated by calling `newPipeline()`. The newly created pipeline will by default contain all the registered repositories and connectors.

```scala
 val pipeline = setl.newPipeline()
```
