package parquet.proto

import java.util.{HashMap, Map}

import com.google.protobuf.Message
import org.apache.hadoop.conf.Configuration
import parquet.hadoop.api.ReadSupport
import parquet.io.api.RecordMaterializer
import parquet.proto.ProtoReadSupport.PB_CLASS
import parquet.schema.MessageType

object SettableProtoReadSupport {
  def setProtoClass(configuration: Configuration, strProtoClass: String) {
    configuration.set(PB_CLASS, strProtoClass)
  }
}

class SettableProtoReadSupport[T <: Message] extends ProtoReadSupport[T] {
  override def prepareForRead(
                               configuration: Configuration,
                               keyValueMetaData: Map[String, String],
                               fileSchema: MessageType,
                               readContext: ReadSupport.ReadContext): RecordMaterializer[T] = {
    val keyValueMetaDataClone = new HashMap[String, String](keyValueMetaData)
    if (!keyValueMetaDataClone.containsKey(PB_CLASS)) {
      keyValueMetaDataClone.put(PB_CLASS, configuration.get(PB_CLASS))
    }
    super.prepareForRead(configuration, keyValueMetaDataClone, fileSchema, readContext)
  }
}