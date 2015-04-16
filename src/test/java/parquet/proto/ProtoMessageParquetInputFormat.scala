package parquet.proto

import com.google.protobuf.GeneratedMessage
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import parquet.hadoop.ParquetRecordReader

class ProtoMessageParquetInputFormat[T <: GeneratedMessage] extends ProtoParquetInputFormat[T] {

  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext) : RecordReader[Void, T] = {
    val reader = super.createRecordReader(inputSplit, taskAttemptContext).asInstanceOf[ParquetRecordReader[_ <: GeneratedMessage.Builder[_]]]

    new MessageRecordReader(reader)
  }
}

class MessageRecordReader[T <: GeneratedMessage](reader: ParquetRecordReader[_ <: GeneratedMessage.Builder[_]]) extends RecordReader[Void, T] {
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = reader.initialize(split, context)

  override def getProgress: Float = reader.getProgress

  override def nextKeyValue(): Boolean = reader.nextKeyValue()

  override def getCurrentValue: T = reader.getCurrentValue.build.asInstanceOf[T]

  override def getCurrentKey: Void = reader.getCurrentKey

  override def close(): Unit = reader.close()
}