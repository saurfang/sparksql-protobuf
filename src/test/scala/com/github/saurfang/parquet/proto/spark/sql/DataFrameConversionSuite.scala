package com.github.saurfang.parquet.proto.spark.sql

import com.github.saurfang.parquet.proto.Simple.SimpleMessage
import com.google.protobuf.ByteString
import org.apache.spark.{SparkConf, SparkContext, LocalSparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{Matchers, FunSuite}
import com.github.saurfang.parquet.proto.spark.sql._

class DataFrameConversionSuite extends FunSuite with LocalSparkContext with Matchers {
  test("convert protobuf with simple data type to dataframe") {
    sc = new SparkContext(new SparkConf().setMaster("local").setAppName("sparksql-protobuf"))
    val sqlContext = new SQLContext(sc)
    val protoRDD = sc.parallelize(
      Seq(
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
      )
    )
    val protoDF = sqlContext.createDataFrame(protoRDD)
    protoDF.collect() shouldBe
      List(
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
      )
  }
}
