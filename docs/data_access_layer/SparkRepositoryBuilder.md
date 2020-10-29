## Description

Based on the same idea of [**ConnectorBuilder**](ConnectorBuilder), [**SparkRepositoryBuilder**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/SparkRepositoryBuilder.scala) helps you create your **SparkRepository** :ok_hand: 

## Usage
Firstly, you should create a configuration file in your project's resources directory.

In this case, let's call it `application.conf`.

```text
csvConfiguration {
  storage = "CSV"
  path = "your/path/to/file.csv"
  inferSchema = "true"
  delimiter = ";"
  header = "true"
  saveMode = "Append"
}
```

Then you can use **ConfigLoader** to load your configuration file. By default it loads `application.conf`.
```scala
val repo = new SparkRepositoryBuilder[MyClass](setl.configLoader.getConfig("csvConfiguration")).getOrCreate()

repo.findAll()
repo.save(dataset)
```

## Parameters
Please refer to [Connector documentation](Connector)