## Definition
A **Stage** is a collection of independent **Factories**. All the stages of a pipeline will be executed sequentially at runtime. Within a stage, all factories could be executed parallelly or sequentially.

## Demo

You could instantiate a stage like the follows: 
```scala
val stage = new Stage()
```

Run in sequential mode:
```scala
stage.parallel(false)
```

Add a factory into this stage:
```scala
// Add an already existed instance of factory
val myFactory = new MyFactory()
stage.addFactory(myFactory)

// Or let the framework handle the instantiation
stage.addFactory(classOf[MyFactory], constructorArguments...)
```

Describe the current stage:
```scala
stage.describe()
```

Run the current stage:
```scala
stage.run()
```