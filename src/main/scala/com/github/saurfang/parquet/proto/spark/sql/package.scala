package com.github.saurfang.parquet.proto.spark

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{AbstractMessage, ByteString, MessageOrBuilder}
import org.apache.spark.sql.types._
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

package object sql {
  /**
   * A JVM-global lock that should be used to prevent thread safety issues when using things in
   * scala.reflect.*.  Note that Scala Reflection API is made thread-safe in 2.11, but not yet for
   * 2.10.* builds.  See SI-6240 for more details.
   */
  protected[sql] object ScalaReflectionLock

  implicit class ProtoSQLContext(sqlContext: SQLContext) {
    /**
     * :: Experimental ::
     * Creates a DataFrame from an RDD of protobuf messages.
     *
     * @group dataframes
     */
    @Experimental
    def createDataFrame[A <: AbstractMessage : TypeTag](rdd: RDD[A]): DataFrame = {
      val schema = ProtoReflection.schemaFor[A].dataType.asInstanceOf[StructType]
      val rowRDD = rdd.map(ProtoRDDConversions.messageToRow)
      sqlContext.createDataFrame(rowRDD, schema)
    }
  }
}
