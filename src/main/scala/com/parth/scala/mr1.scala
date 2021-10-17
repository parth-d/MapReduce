package com.parth.scala


import com.typesafe.config.ConfigFactory

import org.apache.commons.beanutils.converters.DateTimeConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.text.SimpleDateFormat
import java.time.LocalTime
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.JavaConverters.*
import scala.collection.mutable.ListBuffer

/**
 * This class is made to execute the first part of the functionality which shows the distribution of different type of messages across predefined time intervals (application.conf).
 * First mapreduce job (Filter + count):
 *  The logic used is to first parse each line and extract the intervals in the matcher. Now, the time is parsed and the log level is parsed.
 *  The string is then matched with the regex pattern defined in the application.conf file and if it matches, the mapper is instructed to write to context the corresponding time interval, log level and 'one' which is an intWritable.
 *  Here, the interval number is a value obtained by an algorithm which produces appropriate groups based on the time interval. This logic can be seen in line 67.
 *  The reducer sums the matched values for each group (time interval, log level).
 * Second mapreduce job (Time_interval correction job):
 *  The logic used is to split each line to extract the interval number and convert it back into mm:ss format to be put in the output. This splitting logic is implemented by the mapper whereas the reducer does not perform any special operation.
 *
 * The final output is in the following format:
 *    Interval start time (mm:ss)  |  Log Level  |  Number of matching strings
 */

object mr1 {
  /**
   * First mapper: Used to group and count accordingly.
   */
  class Mapper1 extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      /**
       * Steps to extract necessary parameters from the context passed from the main method.
       */
      val interval_length = context.getConfiguration.get("Interval").toInt
      val config_pattern = context.getConfiguration.get("pattern_match")

      /**
       * Logic to parse data into corresponding groups.
       */
      val grouper = Pattern.compile("(.*) \\[.*\\] (INFO|ERROR|WARN|DEBUG).*- (.*)")
      val matcher = grouper.matcher(value.toString)
      if (matcher.find()){
        /**
         * Logic to match the string with the predefined regex pattern.
         */
        val pattern_match = Pattern.compile(config_pattern)
        val pattern_matcher = pattern_match.matcher(matcher.group(3))
        if (pattern_matcher.find()){
          /**
           * If matched, write to context, the appropriate group and "one", which will then be used by the reducer to count.
           */
          val parsed_time = new SimpleDateFormat("HH:mm:ss.SSS").parse(matcher.group(1))
          val group_number = parsed_time.getTime.toInt/(1000*interval_length)
          val group = group_number.toString + "\t" + matcher.group(2)
          context.write(new Text(group), one)
        }
      }
    }
  }

  /**
   * First reducer: Used to count the matching strings groupwise.
   */
  class Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /**
   * Second mapper: Used to convert the group number into the start time of the intervals.
   */
  class Mapper2 extends Mapper[Object, Text, Text, IntWritable]{
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit ={

      /**
       * Parse the value to extract the group and count.
       */
      val pattern = Pattern.compile("(.*)\\s+(.*)")
      val matcher = pattern.matcher(value.toString)

      if (matcher.find())
        /**
         * Reverse engineer to find the start time of the interval and pass it to the reducer.
         */
        val intervals = context.getConfiguration.get("Interval").toInt
        val cur_key = matcher.group(1).toString.split("\\s+")
        val date = new Date(cur_key(0).toLong * intervals * 1000L)
        val level = cur_key(1).toString
        val time = new SimpleDateFormat("mm:ss").format(date)
        context.write(new Text(time.toString + "\t" + level), new IntWritable(matcher.group(2).toInt))
    }
  }

  /**
   * Second reducer: Used to count the matching strings groupwise.
   */
  class Reducer2 extends Reducer[Text, IntWritable, Text, IntWritable]{
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      values.forEach(text => {
        context.write(key, text)
      })
    }
  }

  /**
   * Driver
   */

  def main(): Unit = {
    import org.apache.hadoop.fs.FileSystem
    val logger = CreateLogger(classOf[GenerateLogData.type])

    /**
     * Extract the necessary parameters from the config file (application.conf)
     */
    logger.info("Loading config values")
    val configfile = ConfigFactory.load()
    val pattern_match = configfile.getString("common.pattern")
    val inp_path = configfile.getString("common.input_path")
    val temp_path = configfile.getString("mr1.temp_path")
    val out_path = configfile.getString("mr1.output_path")
    val interval = configfile.getString("common.time_interval")
    logger.info("Loaded config values")

    /**
     * Setup the configuration to be used to pass appropriate values in the context for the job.
     */
    val configuration = new Configuration
    configuration.set("Interval", interval)
    configuration.set("pattern_match", pattern_match)

    /**
     * Logic to delete temporary output folder if exists.
     */
    val fs = FileSystem.get(configuration)
    logger.info("Checking for existing output data.")
    if (fs.exists(new Path(temp_path))) fs.delete(new Path(temp_path), true)

    /**
     * Setup the job with the first mapper and reducer.
     */
    logger.info("Starting first job.")
    val filter_job = Job.getInstance(configuration, "word count")
    filter_job.setJarByClass(this.getClass)
    filter_job.setMapperClass(classOf[Mapper1])
    filter_job.setCombinerClass(classOf[Reducer1])
    filter_job.setReducerClass(classOf[Reducer1])
    filter_job.setOutputKeyClass(classOf[Text])
    filter_job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(filter_job, new Path(inp_path))
    FileOutputFormat.setOutputPath(filter_job, new Path(temp_path))
    if (filter_job.waitForCompletion(true)) {
      /**
       * Second mapreduce job. Logic to delete temporary output folder if exists.
       */
      logger.info("Checking for existing output data.")
      if (fs.exists(new Path(out_path))) fs.delete(new Path(out_path), true)

      /**
       * Setup the configuration to be used to pass appropriate values in the context for the job.
       */
      val config2 = new Configuration()
      config2.set("Interval", interval)

      /**
       * Setup the job with the second mapper and reducer.
       */
      logger.info("Starting second job.")
      val time_job = Job.getInstance(config2, "Time interval correction")
      time_job.setJarByClass(this.getClass)
      time_job.setMapperClass(classOf[Mapper2])
      time_job.setReducerClass(classOf[Reducer2])
      time_job.setOutputKeyClass(classOf[Text])
      time_job.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(time_job, new Path(temp_path))
      FileOutputFormat.setOutputPath(time_job, new Path(out_path))
      System.exit(if (time_job.waitForCompletion(true)) 0 else 1)
    }
  }
}