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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A simple class that represents a winrate job.
 */
public class WinRate extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(WinRate.class);

  /** Flag on the player_slot field that indicates they were on the Dire team. */
  private static final int DIRE_MASK = 128;

  /**
   * Some useful counters to keep track of while processing match history.
   */
  static enum WinRateCounters {
    MALFORMED_MATCH_LINES,
    NON_MATCHMAKING_MATCHES,
    INVALID_MODE_GAMES, // Currently game_mode seems to always be 0, making this useless.
    ABANDONED_GAMES,
    COOP_BOT_GAMES,
    BOT_GAMES, // Game is 'real' but has bots.
    TOURNAMENT_GAMES,
  }

  /*
   * A mapper that reads over matches from the input files and outputs <hero id, match result>
   * key value pairs where a 1 is a victory and a 0 is a loss.
   */
  public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private final static IntWritable WIN = new IntWritable(1);
    private final static IntWritable LOSS = new IntWritable(0);

    private final static JsonParser PARSER = new JsonParser();
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        boolean abandoned = false;
        JsonObject matchTree = PARSER.parse(value.toString()).getAsJsonObject();
        int lobby_type = matchTree.get("lobby_type").getAsInt();
        // This check ensure that we only consider public matchmaking games.
        if (4 == lobby_type) {
          context.getCounter(WinRateCounters.COOP_BOT_GAMES).increment(1);
          // Bot games don't count.
          return;
        } else if (2 == lobby_type) {
          // Keep track of tournament games
          context.getCounter(WinRateCounters.TOURNAMENT_GAMES).increment(1);
        }
        boolean radiant_win = matchTree.get("radiant_win").getAsBoolean();
        // Go through the player list and output the appropriate result information.
        for (JsonElement playerElem : matchTree.get("players").getAsJsonArray()) {
          JsonObject player = playerElem.getAsJsonObject();
          // Check for abandoned and bot games.
          if (!player.has("leaver_status")) {
            // Indicates a bot.
            context.getCounter(WinRateCounters.BOT_GAMES).increment(1);
          } else if (2 == player.get("leaver_status").getAsInt()) {
            // Player abandoned the game. Record it.
            context.getCounter(WinRateCounters.ABANDONED_GAMES).increment(1);
          }
          
          // This is where we actually write out victory or defeat.
          IntWritable hero_id = new IntWritable(player.get("hero_id").getAsInt());
          int player_slot = player.get("player_slot").getAsInt();
          if ((player_slot & DIRE_MASK) != DIRE_MASK) {
            // Radiant player.
            context.write(hero_id, (radiant_win ? WIN : LOSS));
          } else {
            // Dire player
            context.write(hero_id, (radiant_win ? LOSS : WIN));
          }
        }
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter(WinRateCounters.MALFORMED_MATCH_LINES).increment(1);
      }
    }
  }

  /**
   * A simple reducer that simply averages the results to find a winrate.
   * TODO: Output hero names instead.
   */
  public static class Reduce
      extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      float sum = 0;
      float count = 0;
      for (IntWritable i : values) {
        sum += i.get();
        count++;
      }
      context.write(key, new FloatWritable(sum/count));
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new WinRate(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "winrate");
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

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

