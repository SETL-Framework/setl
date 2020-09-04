## Definition

**Condition** is used by the `findBy` method of a **Repository**

```scala
val cond = Set(
  Condition("column1", ">", 100),
  Condition("column2", "=", "value2")
)

myRepository.findBy(cond)
```

## Operation
- `>`
- `<`
- `>=`
- `<=`
- `=`