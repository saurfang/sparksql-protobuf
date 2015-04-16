import com.example.test.Example.Person
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import parquet.hadoop.ParquetInputFormat
import parquet.proto.{ProtoParquetRDD, ProtoMessageParquetInputFormat, SettableProtoReadSupport}
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

    personsDF.agg(Map("id"->"max")).collect() should be === Array(Row(2))

    personsDF.rdd.map(_.getString(0)).collect.toList should be === List("Bob", "Alice")

    personsDF.save("persons.parquet", SaveMode.Overwrite)

    val personsPB = new ProtoParquetRDD(sc, "persons.parquet", classOf[Person])

    personsPB.collect().foreach(println)
    personsPB.collect()(0) should be === Person.newBuilder().setEmail("bob@gmail.com").setId(1).setName("Bob").build

  }
}
