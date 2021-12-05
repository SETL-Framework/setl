# Definition

[**Repository**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/repository/Repository.scala) is a higher level, typed abstraction (compared to **Connector**) of data access layer (DAL) that provides read/write functionalities. It acts as a bridge between a typed *Spark Dataset* and a data store.

The basic repository is defined as follows:
```scala
trait Repository[DT] {

  /**
    * Find data by giving a set of conditions
    *
    * @param conditions Set of [[Condition]]
    * @return
    */
  def findBy(conditions: Set[Condition]): DT

  /**
    * Find data by giving a single condition
    *
    * @param condition a [[Condition]]
    * @return
    */
  def findBy(condition: Condition): DT = this.findBy(Set(condition))

  /**
    * Retrieve all data
    *
    * @return
    */
  def findAll(): DT

  /**
    * Save some data into a data persistence store
    *
    * @param data    data to be saved
    * @param suffix  an optional string to separate data
    * @return
    */
  def save(data: DT, suffix: Option[String]): this.type
}
```

## Implementation
[![](https://mermaid.ink/img/eyJjb2RlIjoiZ3JhcGggVEQ7XG4gIFJlcG9zaXRvcnktLT5TcGFya1JlcG9zaXRvcnk7IiwibWVybWFpZCI6eyJ0aGVtZSI6ImRlZmF1bHQifSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoiZ3JhcGggVEQ7XG4gIFJlcG9zaXRvcnktLT5TcGFya1JlcG9zaXRvcnk7IiwibWVybWFpZCI6eyJ0aGVtZSI6ImRlZmF1bHQifSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)
    
## SparkRepository
[**SparkRepository**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/repository/SparkRepository.scala) is an implementation of **Repository**. It uses [**Connector**](Connector) to access a given persistence storage.

The following code show how to create a repository of a AWS DynamoDB table:
```scala
case class MyObject(a: String, b: Int)
val dynamoDBConnector: DynamoDBConnector = new DynamoDBConnector(spark, region, table, saveMode)
val sparkRepository: SparkRepository = new SparkRepository[MyObject].setConnector(dynamoDBConncetor)

sparkRepository.findAll()
```

**SparkRepository** can handle fields annotated by **ColumnName** or **CompoundKey**.

For example, with a case class defined as follows:
```scala
case class MyObject(@ColumnName("col1") @CompoundKey("partition", "2") column1: String, 
                    @CompoundKey("partition", "1") column2: String,
                    @CompoundKey("clustering", "1") column3: String)
                 
val ds: Dataset[MyObject] = Seq(MyObject("a", "A", "1"), MyObject("b", "B", "2")).toDS()
 // +-------+-------+-------+
 // |column1|column2|column3|
 // +-------+-------+-------+
 // |      a|      A|      1|
 // |      b|      B|      2|
 // +-------+-------+-------+  
```

If we save this dataset with a **SparkRepository**, we will have:
```scala
myRepository.save(ds)
// +-------+-------+-------+--------------+---------------+
// |   col1|column2|column3|_partition_key|_clustering_key|
// +-------+-------+-------+--------------+---------------+
// |      a|      A|      1|           A-a|              1|
// |      b|      B|      2|           B-b|              2|
// +-------+-------+-------+--------------+---------------+  
```

As we may observe, firstly the field **column1** is renamed to **col1** in the persisted table. Then two additional columns are created (`_partition_key` and `_clustering_key`) according to the **CompoundKey** annotation in the case class definition.

This functionality could be useful in some cases where the number of key columns is limited (e.g. in DynamoDB)

For more detail, refer to [Annotation doc](../Annotations)

### Read cache
You can make **SparkRepository** caching the last read data by setting `persistReadData` to *true*. Then SparkRepository will automatically cache the data by invoking the `persist(StorageLevel)` method of DataFrame. 

In this case, if we call multiple times the `findAll()` method or the `findBy()` method with the same condition, SparkRepository will re-use the cached last read data instead of loading these data from the data storage.

Example: 
```scala
mySparkRepository.persistReadData(true)

mySparkRepository.findAll()  // load data from disk
mySparkRepository.findAll()  // load data from cache

mySparkRepository.findBy(myCondition)  // load data from disk as the condition changes
mySparkRepository.findBy(myCondition2)  // load data from disk as the condition differs from the previous one
mySparkRepository.findBy(myCondition2)  // load data from cache

mySparkRepository.save(data)  // read cache will be cleared when save is called
mySparkRepository.findBy(myCondition2)  // load data from disk
```
