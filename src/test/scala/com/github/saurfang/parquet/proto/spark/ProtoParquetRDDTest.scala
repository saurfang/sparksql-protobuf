package com.github.saurfang.parquet.proto.spark

import com.github.saurfang.parquet.proto.AddressBook._
import com.github.saurfang.parquet.proto.spark.sql.ProtoReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest._

class ProtoParquetRDDTest extends FlatSpec with Matchers with BeforeAndAfterAll with Logging{
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override def beforeAll() = {
    sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("sparksql-protobuf")
    )

    sqlContext = new SQLContext(sc)
  }

  override def afterAll() = {
    sc.stop()
  }

  it should "read parquet data as protobuf object" in {
    val rawPersons = sc.parallelize(
      Seq(
        Row("Bob", 1, "bob@gmail.com",
          Seq(Row("1234", Person.PhoneType.HOME.toString), Row("2345", Person.PhoneType.MOBILE.toString)),
          Seq()
        ),
        Row("Alice", 2, "alice@outlook.com", Seq(), Seq("NYC", "Seattle"))
      )
    )

    val personSchema = ProtoReflection.schemaFor[Person].dataType.asInstanceOf[StructType]

    val personsDF = sqlContext.createDataFrame(rawPersons, personSchema)

    personsDF.agg(Map("id" -> "max")).collect() shouldBe Array(Row(2))
    personsDF.rdd.map(_.getString(0)).collect().toList shouldBe List("Bob", "Alice")

    personsDF.save("persons.parquet", SaveMode.Overwrite)


    val personsPB = new ProtoParquetRDD(sc, "persons.parquet", classOf[Person]).collect()
    personsPB.foreach(p => logInfo(p.toString))

    personsPB(0) should ===(Person.newBuilder()
      .setEmail("bob@gmail.com")
      .setId(1)
      .setName("Bob")
      .addPhone(
        Person.PhoneNumber.newBuilder()
          .setNumber("1234")
          .setType(Person.PhoneType.HOME)
      )
      .addPhone(
        Person.PhoneNumber.newBuilder()
          .setNumber("2345")
          .setType(Person.PhoneType.MOBILE)
      )
      .build)

    personsPB(1).getAddressList.toArray(Array.empty[String]) shouldBe Array("NYC", "Seattle")
  }
}
