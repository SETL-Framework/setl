# DataCorp Spark SDK
This project provides a general-proposed framework for data transformation application.

## Use
### Maven
```xml
<!--JCDecaux Datacorp-->
<dependency>
  <groupId>com.jcdecaux.datacorp</groupId>
  <artifactId>dc-spark-sdk_${spark.compat.version}</artifactId>
  <version>0.3.0</version>
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

### Access data storage with SparkRepository
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
     import com.jcdecaux.datacorp.spark.annotation._
     import org.apache.spark.sql.{Dataset, SparkSession}
     import com.jcdecaux.datacorp.spark.SparkSessionBuilder
     import com.jcdecaux.datacorp.spark.config.ConfigLoader
     import com.jcdecaux.datacorp.spark.storage.SparkRepositoryBuilder
     import com.jcdecaux.datacorp.spark.storage.Condition
  
     object Properties extends ConfigLoader
  
     val spark = new SparkSessionBuilder().setEnv("dev").build().get()
     import spark.implicits._
  
     case class MyObject(@ColumnName("col1") @CompoundKey("2") column1: String, 
                         @CompoundKey("1") column2: String)
                      
     val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
      // +-------+-------+
      // |column1|column2|
      // +-------+-------+
      // |      a|      A|
      // |      b|      B|
      // +-------+-------+  

     val repository = new SparkRepositoryBuilder[MyObject](Properties.getConfig("csv"))
       .setSpark(spark).build().get()
    
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
    
### Transform data with Factory and Transformer
The abstract class `com.jcdecaux.datacorp.spark.transformation.Factory` is the basic model of a data
transformation job.

We can create a Factory class by extending the abstract Factory
````scala
import com.jcdecaux.datacorp.spark.transformation.Factory
import com.jcdecaux.datacorp.spark.annotation.Delivery

case class Product1(x: String)
class ProductFactory extends Factory[Product1] {

  var input1: Int = 888

  @Delivery
  var id: String = _
  var output: Product1 = _

  override def read(): this.type = {
    println(input1)
    this
  }
  override def process(): this.type = {
    output = Product1(id)
    this
  }

  override def write(): this.type = this

  override def get(): Product1 = output
}
````

#### What should be done in a Factory class

A factory contains 3 steps:
- read
- process
- write

##### Read
During the read step, you should use `Repository` to read data from data storage

##### Process
Code with business logic should be implemented here. 

##### Write
If output should be saved into a data storage, the code should be implemented here

##### Get
Return the product of your factory

##### Input and parameters
When input data are needed in a factory, they could be provided by:
- manual setting from during the runtime
- handled automatically by pipeline with the @Delivery annotation 
 
The `@Delivery` annotation on the variable `id` means that this variable is an input and it will be given by the `DispatchManager`
in a `Pipeline` (however if you run the factory manually, you should give the value yourself)


#### Transformer TBD

### Handle workflow with Pipeline and Stage
`Pipeline` and `Stage` will help us organise our job and handle the data transfer between different factories.

Imagine we have three data transformation job:
1. Create an object `Product`
2. Create a dataset from step 1
3. Add a new column to the dataset of step 2 and return a `Dataset[Product2]`

This could be done with Pipeline, Stage and Factory

```scala
import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.transformation.Factory
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import com.jcdecaux.datacorp.spark.workflow.{Pipeline, Stage}

// Define our class
case class Product1(x: String)

case class Product2(x: String, y: String)

class ProductFactory extends Factory[Product1] {
  @Delivery 
  var id: String = _  // this is input, and it will be delivered by Pipeline
  var output: Product1 = _
  override def read(): this.type = this
  override def process(): this.type = {
    output = Product1(id)
    this
  }
  override def write(): this.type = this
  override def get(): Product1 = output
}

class DatasetFactory(spark: SparkSession) extends Factory[Dataset[Product1]] {

  import spark.implicits._

  @Delivery
  var p1: Product1 = _  // p1 should be produced by our ProductFactory, pipeline will handle the data transfer
  var output: Dataset[Product1] = _
  override def read(): this.type = this
  override def process(): this.type = {
    output = Seq(p1, Product1("pd1")).toDS
    this
  }
  override def write(): this.type = this
  override def get(): Dataset[Product1] = output
}

class DatasetFactory2(spark: SparkSession) extends Factory[Dataset[Product2]] {

  @Delivery
  var ds: Dataset[Product1] = _  // ds should be produced by DatasetFactory, pipeline will handle the data transfer
  var output: Dataset[Product2] = _
  override def read(): this.type = this
  override def process(): this.type = {
    import spark.implicits._
    output = ds.withColumn("y", functions.lit("c2")).as[Product2]
    this
  }
  override def write(): this.type = {
    output.show()
    this
  }
  override def get(): Dataset[Product2] = {
    output
  }
}
```

Now our classes are defined, let implement the complete data job:

```scala
import com.jcdecaux.datacorp.spark.SparkSessionBuilder
import com.jcdecaux.datacorp.spark.transformation.Deliverable
import com.jcdecaux.datacorp.spark.workflow.{Pipeline, Stage}

val spark = new SparkSessionBuilder("dev").setEnv("dev").getOrCreate()
import spark.implicits._

// Instantiate our classes
val f1 = new ProductFactory
val f2 = new DatasetFactory(spark)
val f3 = new DatasetFactory2(spark)
val pipeline = new Pipeline
val stage0 = new Stage().addFactory(f1)
val stage1 = new Stage().addFactory(f2)
val stage2 = new Stage().addFactory(f3)

pipeline
  .setInput(
    new Deliverable[String]("id_of_product1")                 // we set manually the input for f1 as 
    .setConsumer(f1.getClass)                                 // it is the first factory of our pipeline
  )                                                                                                             
  .setInput(new Deliverable[String]("wrong_id_of_product1"))  // if multiple input has the same type, then you should 
                                                              // specify the consumer of this input
  .addStage(stage0)
  .addStage(stage1)
  .addStage(stage2)
  .run()
  
f3.get().show()
//+--------------+---+
//|             x|  y|
//+--------------+---+
//|id_of_product1| c2|
//|           pd1| c2|
//+--------------+---+
```

You can also call the `pipeline.describe()` method to generate a pipeline summary
```
========== Pipeline Summary ==========

----------   Nodes Summary  ----------
Node   : ProductFactory
Stage  : 0
Input  : String
Output : com.jcdecaux.datacorp.spark.workflow.Product1
--------------------------------------
Node   : DatasetFactory
Stage  : 1
Input  : com.jcdecaux.datacorp.spark.workflow.Product1
Output : org.apache.spark.sql.Dataset[com.jcdecaux.datacorp.spark.workflow.Product1]
--------------------------------------
Node   : DatasetFactory2
Stage  : 2
Input  : org.apache.spark.sql.Dataset[com.jcdecaux.datacorp.spark.workflow.Product1]
Output : org.apache.spark.sql.Dataset[com.jcdecaux.datacorp.spark.workflow.Product2]
--------------------------------------
----------   Flows Summary  ----------
Flow
Stage     : 0
Direction : ProductFactory ==> DatasetFactory
PayLoad   : com.jcdecaux.datacorp.spark.workflow.Product1
--------------------------------------
Flow
Stage     : 1
Direction : DatasetFactory ==> DatasetFactory2
PayLoad   : org.apache.spark.sql.Dataset[com.jcdecaux.datacorp.spark.workflow.Product1]
--------------------------------------

```

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
