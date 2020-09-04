## Description
A **Factory[A]** is a complete data transformation job to produce an object of type A. 

## Difference with *Transformer*
A **Factory** is more complex than a **Transformer**. In addition to data transformation, a **Factory** contains also logics for reading and writing data.

## Demo
You could implement your own factory by extending the class **Factory[A]**.

```scala
case class MyProduct

// MyFactory will produce MyProduct
class MyFactory extend Factory[MyProduct] {
  override def read(): this.type = ...
  override def process(): this.type = ...
  override def write(): this.type = ...
  override def get(): MyProduct = ...
}
```

To run **MyFactory**:
```scala
new MyFactory().read().process().write().get()
```

## Dependency Handling
Dependency of a **Factory** could be handled by a **Pipeline** if the field has the **Delivery** annotation.
For the previous **MyFactory** class:

```scala
case class MyProduct

// MyFactory will produce MyProduct
class MyFactory extend Factory[MyProduct] {
  
  @Delivery
  var input: String = _

  override def read(): this.type = ...
  override def process(): this.type = ...
  override def write(): this.type = ...
  override def get(): MyProduct = ...
}
```

By adding `@Delivery` to the variable **input**, the value of **input** will be automatically injected by **Pipeline**.

For more information about dependency handling, read the [doc of **Pipeline**](Pipeline).
