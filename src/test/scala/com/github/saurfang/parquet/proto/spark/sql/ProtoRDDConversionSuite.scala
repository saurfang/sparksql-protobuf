package com.github.saurfang.parquet.proto.spark.sql

import com.github.saurfang.parquet.proto.AddressBook.Person
import com.github.saurfang.parquet.proto.AddressBook.Person.{EmptyMessage, PhoneNumber}
import com.github.saurfang.parquet.proto.Simple.SimpleMessage
import com.google.protobuf.ByteString
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{FunSuite, Matchers}
import ProtoRDDConversions._

class ProtoRDDConversionSuite extends FunSuite with Matchers {
  test("convert protobuf with simple data type to dataframe") {
    val protoMessage =
      SimpleMessage.newBuilder()
        .setBoolean(true)
        .setDouble(1)
        .setFloat(1F)
        .setInt(1)
        .setLong(1L)
        .setFint(2)
        .setFlong(2L)
        .setSfint(3)
        .setSflong(3L)
        .setSint(-4)
        .setSlong(-4)
        .setString("")
        .setUint(5)
        .setUlong(5L)
        .build

    val protoRow = messageToRow(protoMessage)
    protoRow shouldBe
      Row(
        1.0, // double
        1.0F, // float
        1, // int
        1L, // long
        5, // uint
        5L, // ulong
        -4, // sint
        -4L, // slong
        2, // fint
        2L, // flong
        3, // sfint
        3L, // sflong
        true, // boolean
        "", // String
        null // ByteString
      )
  }

  test("convert protobuf with byte string") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val protoMessage =
        SimpleMessage.newBuilder()
          .setByteString(ByteString.copyFrom(bytes))
          .build
    messageToRow(protoMessage).toSeq.last shouldBe bytes
  }

  test("convert protobuf with repeated fields") {
    val protoMessage =
      Person.newBuilder()
        .setName("test")
        .setId(0)
        .addAddress("ABC")
        .addAddress("CDE")
        .addPhone(PhoneNumber.newBuilder().setNumber("12345").setType(Person.PhoneType.MOBILE))
        .build
    val protoRow = messageToRow(protoMessage)
    protoRow shouldBe Row("test", 0, null, Seq(Row("12345", "MOBILE")), Seq("ABC", "CDE"), null)
  }

  test("convert protobuf with empty repeated fields") {
    val protoMessage = Person.newBuilder().setName("test").setId(0).build()
    val protoRow = messageToRow(protoMessage)
    protoRow shouldBe Row("test", 0, null, Seq(), Seq(), null)
  }
}
