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
package parquet.proto.utils;


import com.example.test.Example.Person;
import com.example.test.Example.PersonOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import parquet.Log;
import parquet.proto.ProtoParquetInputFormat;
import parquet.proto.ProtoReadSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * Reads data from given parquet file using MapReduce job.
 */
public class ReadUsingMR {
    private static final Log LOG = Log.getLog(ReadUsingMR.class);


    private static List<Person> outputPersons;

    Configuration conf = new Configuration();
    private String projection;

    public void setRequestedProjection(String projection) {
        this.projection = projection;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public static class ReadingMapper extends Mapper<Void, PersonOrBuilder, LongWritable, Person> {
        protected void map(Void key, PersonOrBuilder value, Context context) throws IOException, InterruptedException {
            Person clone = ((Person.Builder) value).build();
            outputPersons.add(clone);
        }
    }

    public List<Person> read(Path parquetPath) throws Exception {

        synchronized (ReadUsingMR.class) {
            outputPersons = new ArrayList<Person>();

            final Job job = new Job(conf, "read");
            job.setInputFormatClass(ProtoParquetInputFormat.class);
            ProtoParquetInputFormat.setInputPaths(job, parquetPath);
            ProtoParquetInputFormat.setReadSupportClass(job, ProtoReadSupport.class);
            if (projection != null) {
                ProtoParquetInputFormat.setRequestedProjection(job, projection);
            }

            job.setMapperClass(ReadingMapper.class);
            job.setNumReduceTasks(0);

            job.setOutputFormatClass(NullOutputFormat.class);

            waitForJob(job);

            List<Person> result = Collections.unmodifiableList(outputPersons);
            outputPersons = null;
            return result;
        }
    }

    static void waitForJob(Job job) throws Exception {
        job.submit();
        while (!job.isComplete()) {
            LOG.debug("waiting for job " + job.getJobName());
            sleep(50);
        }
        LOG.debug("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
        if (!job.isSuccessful()) {
            throw new RuntimeException("job failed " + job.getJobName());
        }
    }

}