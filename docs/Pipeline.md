### Handle workflow with Pipeline and Stage
`Pipeline` and `Stage` will help us organize our job and handle the data transfer between different factories.

Imagine we have three data transformation jobs in an ETL application:
1. Transformation 1: Create an object `Product`
2. Transformation 2: Create a dataset of `Product` with the output of transformation 1
3. Transformation 3: Add a new column to the dataset of transformation 2 and return a `Dataset[Product2]`

This could be done with Pipeline, Stage, and Factory

```scala
import io.github.setl.SparkSessionBuilder
import io.github.setl.annotation.Delivery
import io.github.setl.transformation.Factory
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import io.github.setl.workflow.{Pipeline, Stage}

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

class DatasetFactory extends Factory[Dataset[Product1]] with HasSparkSession {

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

class DatasetFactory2 extends Factory[Dataset[Product2]] with HasSparkSession {

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
val setl = Setl.builder()
  .withDefaultConfigLoader("your_conf.conf")
  .getOrCreate()

// Instantiate our classes
val f1 = new ProductFactory
val f2 = new DatasetFactory
val f3 = new DatasetFactory2

setl
  .newPipeline()
  .setInput(
    new Deliverable[String]("id_of_product1")                 // we set manually the input for f1 as 
    .setConsumer(f1.getClass)                                 // it is the first factory of our pipeline
  )                                                                                                             
  .setInput(new Deliverable[String]("wrong_id_of_product1"))  // if multiple input has the same type, then you should 
                                                              // specify the consumer of this input
  .addStage(f1)
  .addStage(f2)
  .addStage(f3)
  .describe()
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
