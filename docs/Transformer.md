The notion of the transformer is preliminary. 

# Definition
**Transformer** is the atomic class for data transformation. A `transformer[T]` will transform some input data into an object of type **T**. 


## When should I use a transformer
The original idea of the transformer is to decouple a complex data processing procedure of a **Factory**. Generally, a transformer should be placed inside a **Factory**. A factory can have multiple transformers.

A transformer should be simple (in terms of task, for example, transform an object of type A to type B) and stateless (which means it should minimize its dependence on the application context).

Another use case would be to implement several different data transformation logic for one factory (for example, there may be several different ML models for one single prediction job). In this case, there should be a way to select the most appropriate transformer according to their performance in a specific environment.



