# DataCorp Spark SDK
This project provides a general-proposed framework for data transformation application.

## Use
### Maven
```xml
<!--JCDecaux Datacorp-->
<dependency>
  <groupId>com.jcdecaux.datacorp</groupId>
  <artifactId>dc-spark-sdk_${spark.compat.version}</artifactId>
  <version>0.2.6</version>
</dependency>
```

Make sure that you have already added **nexus.jcdecaux.com** into the project repositories. 
Otherwise, add to your `pom.xml`
```xml
<repositories>
  <repository>
    <id>nexus-datacorp-snapshots</id>
    <url>http://nexus.jcdecaux.com/repository/snapshots-DATACORP/</url>
  </repository>
  <repository>
    <id>nexus-datacorp-releases</id>
    <url>http://nexus.jcdecaux.com/repository/releases-DATACORP/</url>
  </repository>
</repositories>
```

## Example

### Data storage layer access
Let's create a csv file.

1. define a configuration file (eg. `application.conf`)
    ```
      csv {
        storage = "CSV"
        path = "file/path"
        inferSchema = "true"
        delimiter = ";"
        header = "true"
        saveMode = "Append"
      }
    ```
2. Code:
    ```scala
     import com.jcdecaux.datacorp.spark.annotations.ColumnName
     import com.jcdecaux.datacorp.spark.annotations.CompoundKey
     
     import org.apache.spark.sql.{Dataset, SparkSession}
     import com.jcdecaux.datacorp.spark.SparkSessionBuilder
     import com.jcdecaux.datacorp.spark.config.ConfigLoader
     import com.jcdecaux.datacorp.spark.storage.SparkRepositoryBuilder
     import com.jcdecaux.datacorp.spark.storage.Condition
  
     object Properties extends ConfigLoader
  
     val spark = new SparkSessionBuilder().setEnv("dev").build().get()
     import spark.implicits._
  
     case class MyObject(@ColumnName("col1") @CompoundKey("2") column1: String, @CompoundKey("1") column2: String)
     val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
      // +-------+-------+
      // |column1|column2|
      // +-------+-------+
      // |      a|      A|
      // |      b|      B|
      // +-------+-------+  

     val repository = new SparkRepositoryBuilder[MyObject](Properties.getObject("csv")).setSpark(spark).build().get()
     repository.save(ds)
     // The column name will be changed automatically according to 
     // the annotation `colName` when you define the case class 
     // +----+-------+-----+
     // |col1|column2| _key|
     // +----+-------+-----+
     // |   a|      A|  A-a|
     // |   b|      B|  B-b|
     // +----+-------+-----+
  
     val cond = Condition("col1", "=", "a")
  
     repository.findBy(cond).show()
     // Dataset[MyObject] 
     // +-------+-------+
     // |column1|column2|
     // +-------+-------+
     // |      a|      A|
     // +-------+-------+  
    ```
    
### Transform data with Factory
TBD

### Handle workflow with Pipeline
TBD

## Build and deployment
Maven is used as the dependency manager in this project.

### Build
```bash
mvn clean package -Pprovided
```

### Deploy
```bash
# SNAPSHOT
mvn -Pprovided clean deploy 

# RELEASE
mvn -Dchangelist= -Pprovided clean deploy 
```
