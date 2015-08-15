package com.github.saurfang.parquet.proto.spark.sql

import com.github.saurfang.parquet.proto.AddressBook.Person
import com.github.saurfang.parquet.proto.Simple.SimpleMessage
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FunSuite}
import ProtoReflection._

class ProtoReflectionSuite extends FunSuite with Matchers {

  test("ProtoReflection should derive correct simple type") {
    schemaFor[SimpleMessage] shouldBe
      Schema(
        StructType(
          Seq(
            StructField("double",DoubleType),
            StructField("float",FloatType),
            StructField("int",IntegerType),
            StructField("long",LongType),
            StructField("uint",IntegerType),
            StructField("ulong",LongType),
            StructField("sint",IntegerType),
            StructField("slong",LongType),
            StructField("fint",IntegerType),
            StructField("flong",LongType),
            StructField("sfint",IntegerType),
            StructField("sflong",LongType),
            StructField("boolean",BooleanType),
            StructField("String",StringType),
            StructField("ByteString",BinaryType)
          )
        ),
        nullable = true
      )
  }

  test("ProtoReflection should derive complex type") {
    schemaFor[Person] shouldBe
      Schema(
        StructType(
          Seq(
            StructField("name", StringType, nullable = false),
            StructField("id", IntegerType, nullable = false),
            StructField("email", StringType),
            StructField("phone",
              ArrayType(
                StructType(
                  Seq(
                    StructField("number", StringType, nullable = false),
                    StructField("type", StringType)
                  )
                ),
                containsNull = false
              ),
              nullable = false
            ),
            StructField("address", ArrayType(StringType, containsNull = false), nullable = false)
          )
        ),
        nullable = true
      )
  }

  test("SchemaFor should fail for other types") {
    intercept[UnsupportedOperationException](schemaFor[StructField])
  }
}
