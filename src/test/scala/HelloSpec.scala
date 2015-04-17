import com.example.test.Example.Person
import com.example.test.Example.Person.{PhoneNumber, PhoneNumberInner, PhoneNumberOuter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import parquet.spark.ProtoParquetRDD

class HelloSpec extends FlatSpec with Matchers {
  "Hello" should "have tests" in {
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("sparksql-protobuf")
    )

    val sqlContext = new SQLContext(sc)

    val rawPersons = sc.parallelize(
      Seq(
        Row("Bob", 1, "bob@gmail.com", Seq(Row("1234", "HOME"), Row("2345", "CELL"))),
        Row("Alice", 2, "alice@outlook.com", Seq())
      )
    )

    val personSchema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("id", IntegerType),
        StructField("email", StringType),
        StructField("phone",
          ArrayType(
            StructType(
              Seq(
                StructField("number", StringType),
                StructField("type", StringType)
              )
            )
          )
        )
      )
    )


    val personsDF = sqlContext.createDataFrame(rawPersons, personSchema)

    personsDF.agg(Map("id" -> "max")).collect() should be === Array(Row(2))

    personsDF.rdd.map(_.getString(0)).collect.toList should be === List("Bob", "Alice")

    personsDF.save("persons.parquet", SaveMode.Overwrite)

    val personsPB = new ProtoParquetRDD(sc, "persons.parquet", classOf[Person])

    personsPB.collect().foreach(println)
    personsPB.collect()(0) should be === Person.newBuilder()
      .setEmail("bob@gmail.com")
      .setId(1)
      .setName("Bob")
      .setPhone(
        PhoneNumberOuter.newBuilder()
          .addBag(
            PhoneNumberInner.newBuilder()
              .setArray(
                PhoneNumber.newBuilder()
                  .setNumber("1234")
                  .setType("HOME")
              )
          )
          .addBag(
            PhoneNumberInner.newBuilder()
              .setArray(
                PhoneNumber.newBuilder()
                  .setNumber("2345")
                  .setType("CELL")
              )
          )
      )
      .build

  }
}
