# Annotations
There are several annotations that we defined in this framework to provide easy-to-use functionality.

## Repository related Annotations

### @ColumnName
The annotation `@ColumnName` can be used when you want to rename some columns in a data store but conserve its original case class field name at the same time.

In such a case, by adding the annotation ColumnName before the field definition, **SparkRepository** will automatically rename these columns while reading/writing the data.

#### Example:
If we define a case class like this:
```scala
case class MyTable(@ColumnName("my_first_col") myFisrtCol: String,
                   @ColumnName("my_second_col") mySecondCol: String)

```

When we write the following spark Dataset, SparkRepository will rename the column **myFirstCol** and **mySecondCol** into **my_first_col** and **my_second_col** respectively when it writes the data. And it will do the opposite when it reads the data.

### @CompoundKey
In some databases, for example, AWS DynamoDB, we can use only one partition key and one clustering key to keep the uniqueness of our data. However, in our spark Dataset, we are not able to guarantee the uniqueness with only two columns.

In this case, by applying the annotation `@CompoundKey(group, order)` before the field declaration in out case class, SparkRepository will create some new columns by concatenating all the annotated columns according to their defined group and order.

#### Example:
```scala
case class MyTable(@CompoundKey("partition", "1") col1: String,
                   @CompoundKey("partition", "2") col2: String,
                   @CompoundKey("clustering", "1") col3: String)
```

With the above definition, when we try to save the following Dataset with **SparkRepository**, two additional columns will be added automatically.

| col1 | col2 | col3 |
|------|------|------|
| a    | A    | 1    |
| b    | B    | 2    |
| c    | C    | 3    |

will be transformed into:

| col1 | col2 | col3 | _partition_key | _clustering_key |
|------|------|------|----------------|-----------------|
| a    | A    | 1    | a-A            | 1               |
| b    | B    | 2    | b-B            | 2               |
| c    | C    | 3    | c-C            | 3               |


### @Compress
When you have a complex structure in a Dataset, you could use the annotation `@Compress` to automatically compress the selected columns.

```scala
case class CompressionDemo(@Compress col1: Seq[Int],
                           @Compress(compressor = classOf[GZIPCompressor]) col2: Seq[Seq[String]])
```

**SparkRepository** will compress the column *col1* with the default compressor (**XZCompressor**) and *col2* with a **GZIPCompressor**, then save them as two binary columns. When it reads the data from the data store, it will decompress these columns and map it back to their original data type.

## Factory related Annotations

### @Delivery
```scala
class Foo extends Factory[Something] {
  @Delivery
  var input1: MyClass = _

  @Delivery(producer = classOf[Factory2])
  var input2: String = _

  @Delivery(producer = classOf[Factory3], optional = true)
  var input3: String = _

  @Delivery(id = "id1")
  var input4: Array[String] = _

  @Delivery(autoLoad = true)
  var input5: Dataset[TestClass] = _

  @Delivery(autoLoad = true, condition = "col1 = 'A'", optional = true)
  var input6: Dataset[TestClass] = _

  @Delivery(autoLoad = true, condition = "col1 = 'A'", id = "id1", optional = true)
  var input7: Dataset[TestClass] = _
}
```

In the above example:
- **input1** will be delivered if there exists an object of MyClass in the delivery pool. Otherwise, a **NoSuchElementException** will be thrown.
- **input2** will be delivered if there exists a String produced by **Factory2** in the delivery pool. Otherwise, a **NoSuchElementException** will be thrown.
- **input3** will be delivered if there exists a String produced by **Factory3**. However, no exception will be thrown if the input is missing.
- **input4** will be delivered if there exists a delivery of type Array[String] that has an id "id1" in the delivery pool. Otherwise, a **NoSuchElementException** will be thrown.
- **input5** will be delivered if there exists a Dataset[TestClass] in the delivery pool. If it's missing, the pipeline will try to deliver this variable by searching for any available *SparkRepository[TestClass]* in the delivery pool. If there is such a repository, then the "findAll" method will be invoked and the returned value will be delivered. If none of those is available, a **NoSuchElementException** will be thrown.
- **input6** will be delivered if there exists a Dataset[TestClass] in the delivery pool. If it's missing, the pipeline will try to invoke the "findBy" method of an available *SparkRepository[TestClass]* with the given condition, and it will deliver the returned value. If none of those is available, this delivery will be omitted.
- **input7** will be delivered like the *input6*. In addition to the conditions of *input6 delivery*, the pipeline will only look for Dataset[TestClass] or SparkRepository[TestClass] with id == "id1". 