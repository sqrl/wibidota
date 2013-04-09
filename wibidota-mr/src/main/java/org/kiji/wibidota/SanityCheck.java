/* Copyright 2013 WibiData, Inc.
*
* See the NOTICE file distributed with this work for additional
* information regarding copyright ownership.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.kiji.wibidota;

import static junit.framework.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A class that represents a job to sanity check that a match id appears no more than once in the
 * input data.
 */
public class SanityCheck extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(SanityCheck.class);

  /*
   * A mapper that reads over matches from the input files and outputs <match id, 1> for each.
   */
  public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private final static JsonParser PARSER = new JsonParser();
    private final static IntWritable ONE = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchTree = PARSER.parse(value.toString()).getAsJsonObject();
        int match_id = matchTree.get("match_id").getAsInt();
        context.write(new IntWritable(match_id), ONE);
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter("dotametrics", "malformed").increment(1);
      }
    }
  }

  /**
   * A simple reducer that simply makes sure no key has more than one value.
   */
  public static class Reduce
      extends Reducer<IntWritable, IntWritable, NullWritable, NullWritable> {
    private final static Gson GSON = new GsonBuilder().create();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable result : values) {
        count++;
      }
      assertTrue(count == 1);
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new SanityCheck(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "sanity");
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return -1;
    }
  }
}

