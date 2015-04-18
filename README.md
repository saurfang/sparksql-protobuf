# sparksql-protobuf

An example project that uses SparkSQL to read raw data and write them in parquet format. 
Then we use a protobuf definition to read them back and we will get an RDD of the protobuf object.

## Motivation

1. Protobuf defines nested data structure easily 
2. It doesn't constraint you to the 22 fields limit in case class (no longer true once we upgrade to 2.11+)
2. Protobuf is language agnostic and can generate code that gives you native object hence you get all the benefit of type checking and code completion unlike operating `Row` in Spark/SparkSQL

## Related Work
[Elephant Bird](https://github.com/twitter/elephant-bird)
