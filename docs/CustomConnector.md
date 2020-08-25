## Custom Connector

You can implement you own data source connector by implementing the `ConnectorInterface`

```scala
class CustomConnector extends ConnectorInterface with CanDrop {
  override def setConf(conf: Conf): Unit = null

  override def setConfig(config: Config): Unit = null

  override def read(): DataFrame = {
    import spark.implicits._
    Seq(1, 2, 3).toDF("id")
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = log.debug("Write with suffix")

  override def write(t: DataFrame): Unit = log.debug("Write")

  /**
   * Drop the entire table.
   */
  override def drop(): Unit = log.debug("drop")
}
```

To use it, just set the storage to **OTHER** and provide the class reference of your connector:

```txt
myConnector {
  storage = "OTHER"
  class = "com.example.CustomConnector"  // class reference of your connector 
}
```
