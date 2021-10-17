package com.parth.scala

import com.typesafe.config.ConfigFactory

import org.apache.commons.beanutils.converters.DateTimeConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, Writable}
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.text.SimpleDateFormat
import java.time.LocalTime
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.JavaConverters.*
import scala.collection.mutable.ListBuffer

/**
 * This class is made to execute the third part of the functionality which will produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.
 * Mapreduce job (Filter + max):
 *  The logic used is to filter each log message based on its logging level and if it matches, pass its string length to the reducer which will find the max value for that type.
 *
 * The final output is in the following format:
 *    Log Level  |  Max length of matching string
 */
object mr4 {

  /**
   * Mapper: Used to group and count accordingly.
   */
  class Mapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      /**
       * Logic to group log messages according to their log level.
       */
      val pattern = Pattern.compile("(INFO|ERROR|WARN|DEBUG).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        val group = matcher.group(1)
        val message = matcher.group(2)
        val pattern_match = Pattern.compile(context.getConfiguration.get("pattern_match"))
        val pattern_matcher = pattern_match.matcher(message)
        if (pattern_matcher.find()){
          /**
           * If matched, write to context, the appropriate group number and length of the string, which will then be used by the reducer.
           */
          context.write(new Text(group), new IntWritable(message.length))
        }
      }
    }
  }

  /**
   * Reducer: Used to find the maximum length of matching strings groupwise.
   */
  class reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val maxv = values.asScala.foldLeft(0){(max, curr) => math.max(max, curr.get)}
      context.write(key, new IntWritable(maxv))
    }
  }

  /**
   * Driver
   */
  def main(args: Array[String]): Unit = {
    import org.apache.hadoop.fs.FileSystem
    val logger = CreateLogger(classOf[GenerateLogData.type])

    /**
     * Extract the necessary parameters from the config file (application.conf)
     */
    logger.info("Loading config values")
    val config = ConfigFactory.load()
    val pattern_match = config.getString("common.pattern")
    val inp_path = config.getString("common.input_path")
    val out_path = config.getString("mr4.output_path")
    logger.info("Loaded config values")
    /**
     * Setup the configuration to be used to pass appropriate values in the context for the job.
     */
    val configuration = new Configuration
    configuration.set("pattern_match", pattern_match)

    /**
     * Logic to delete temporary output folder if exists.
     */
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(out_path))) fs.delete(new Path(out_path), true)

    /**
     * Setup the job with the mapper and reducer.
     */
    logger.info("Starting the job.")
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Mapper])
    job.setCombinerClass(classOf[reducer])
    job.setReducerClass(classOf[reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(inp_path))
    FileOutputFormat.setOutputPath(job, new Path(out_path))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}