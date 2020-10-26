## Custom Connector

You can implement you own data source connector by implementing the `ConnectorInterface`

```scala
import com.jcdecaux.setl.storage.connector.ConnectorInterface
import com.jcdecaux.setl.internal.CanDrop
import com.jcdecaux.setl.config.Conf
import org.apache.spark.sql.DataFrame

class CustomConnector extends ConnectorInterface with CanDrop {
  override def setConf(conf: Conf): Unit = {
    // configuration
  }

  override def read(): DataFrame = {
    import spark.implicits._
    Seq(1, 2, 3).toDF("id")
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = logDebug("Write with suffix")

  override def write(t: DataFrame): Unit = logDebug("Write")

  override def drop(): Unit = logDebug("drop")
}
```

### Functionalities

Like the previous example, by extending your connector class with functionality traits (like `CanDrop`) 
and implementing accordingly their abstract methods, SparkRepository will be able to use these specific
functionalities.

### Use the custom connector

To use this connector, set the storage to **OTHER** and provide the class reference of your connector:

```txt
myConnector {
  storage = "OTHER"
  class = "com.example.CustomConnector"  // class reference of your connector
  yourParam = "some parameter" // put your parameters here
}
```
