import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SQLContext, Row}
import org.apache.spark.sql.types.{StructType, IntegerType, StructField, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import parquet.hadoop.ParquetInputFormat
import parquet.proto.utils.ReadUsingMR

class HelloSpec extends FlatSpec with ShouldMatchers {
  "Hello" should "have tests" in {
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("sparksql-protobuf")
    )

    val sqlContext = new SQLContext(sc)

    val rawPersons = sc.parallelize(
      Seq(
        Row("Bob", 1, "bob@gmail.com"),
        Row("Alice", 2, "alice@outlook.com")
      )
    )

    val personSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("email", StringType, nullable = true)
      )
    )

    val personsDF = sqlContext.createDataFrame(rawPersons, personSchema)

    personsDF.agg(Map("id"->"max")).printSchema()

    println(personsDF.rdd.map(_.getString(0)).collect.toList)
//
//    personsDF.save("org.apache.spark.sql.parquet",
//      SaveMode.Overwrite,
//      Map(
//        "path" -> "persons.parquet",
//        "parquet.proto.class" -> "com.example.test.Example.Person"
//      )
//    )
//
//    val reader = new ReadUsingMR
//    val persons = reader.read(new Path("persons.parquet"))
//

    true should be === true
  }
}
