/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.saurfang.parquet.proto;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Log;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.schema.MessageType;

/**
 * Custom read support that support nested parquet written by SparkSQL
 */
public class ProtoLISTReadSupport<T extends Message> extends ProtoReadSupport<T> {
    private static final Log LOG = Log.getLog(ProtoLISTReadSupport.class);

    public ProtoLISTReadSupport() {
    }

    public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        String headerProtoClass = (String)keyValueMetaData.get("parquet.proto.class");
        String configuredProtoClass = configuration.get("parquet.proto.class");
        if(configuredProtoClass != null) {
            LOG.debug("Replacing class " + headerProtoClass + " by " + configuredProtoClass);
            headerProtoClass = configuredProtoClass;
        }

        if(headerProtoClass == null) {
            throw new RuntimeException("I Need parameter parquet.proto.class with Protocol Buffer class");
        } else {
            LOG.debug("Reading data with Protocol Buffer class " + headerProtoClass);
            MessageType requestedSchema = readContext.getRequestedSchema();
            Class protobufClass = Protobufs.getProtobufClass(headerProtoClass);
            return new ProtoLISTRecordMaterializer(requestedSchema, protobufClass);
        }
    }
}
