package parquet.spark

import com.google.protobuf.AbstractMessage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import parquet.hadoop.ParquetInputFormat
import parquet.proto.{ProtoMessageParquetInputFormat, SettableProtoReadSupport}

import scala.reflect.ClassTag

class ProtoParquetRDD[T <: AbstractMessage : ClassTag](
                                           sc: SparkContext,
                                           input: String,
                                           protoClass: Class[T],
                                           @transient conf: Configuration
                                             ) extends RDD[T](sc, Nil){

  def this(sc: SparkContext,input: String,protoClass: Class[T]) = {
    this(sc, input, protoClass, sc.hadoopConfiguration)
  }

  lazy private[this] val rdd = {
    val jconf = new JobConf(conf)
    FileInputFormat.setInputPaths(jconf, input)
    ParquetInputFormat.setReadSupportClass(jconf, classOf[SettableProtoReadSupport[T]])
    SettableProtoReadSupport.setProtoClass(jconf, protoClass.getName)

    new NewHadoopRDD(sc, classOf[ProtoMessageParquetInputFormat[T]], classOf[Void], protoClass, jconf)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = rdd.compute(split, context).map(_._2)

  override protected def getPartitions: Array[Partition] = rdd.getPartitions
}
