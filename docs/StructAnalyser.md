## Definition

**StructAnalyser** provides functionalities to retrieve annotation information from a class.

It scans the class' metadata and returns a **StructType** so that the **SchemaConverter** could use to transform the schema of a DataFrame/Dataset.

You can access the metadata of your class by getting the metadata of **StructField**

### Demo

```scala
case class MyClass(col1: String, @ColumnName("column_2") col2: String)

// analyseSchema will return a StructType of MyClass
val structType = StructAnalyser.analyseSchema[MyClass]
```
