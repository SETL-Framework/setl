## Definition
[**ConnectorBuilder**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/ConnectorBuilder.scala) provides a simplified way to create **Connector**.

## Usage
You have two ways to instantiate a **ConnectorBuilder**:
- with a *Typesafe* [**Config**](https://github.com/lightbend/config) object from a configuration file
- with a [**Conf**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/config/Conf.scala) object from a `Map[String, String]`.

### With Typesafe Config
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

Then you can use **ConfigLoader** to load your configuration file. By default, it loads `application.conf`.
```scala
object Properties extends ConfigLoader

val connector = new ConnectorBuilder(spark, Properties.getConfig("csvConfiguration")).getOrCreate()

connector.read()
connector.write(df)
```

### With Conf
You can create a **Conf** object from a **Map**.
```scala
val conf = Conf.fromMap(
  Map(
    "storage" -> "PARQUET",
    "path" -> "path/to/your/file",
    ...
  )
)

val connector = new ConnectorBuilder(spark, conf).getOrCreate()

connector.read()
connector.write(df)

```

## Parameters
Please refer to [Connector documentation](data_access_layer/Connector)