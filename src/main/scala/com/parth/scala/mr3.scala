package com.parth.scala

import com.typesafe.config.ConfigFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import HelperUtils.CreateLogger

import java.util.regex.Pattern
import java.lang.Iterable

import scala.collection.JavaConverters.*

/**
 * This class is made to execute the third part of the functionality which for each message type will produce the number of the generated log messages.
 * Mapreduce job (Filter + count):
 *  The logic used is to first parse each line and extract the log level in the matcher.
 *  The mapper is instructed to write to context the corresponding level and 'one' which is an intWritable.
 *  The reducer sums the matched values for each group (log level).
 *
 * The final output is in the following format:
 *    Log Level  |  Number of generated log messages
 */
object mr3 {
  /**
   * Mapper: Used to group and count accordingly.
   */
  class mapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      /**
       * Logic to group log messages according to their log level.
       */
      val pattern = Pattern.compile("(INFO|ERROR|WARN|DEBUG).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        /**
         * Write to context, the appropriate group number and "one", which will then be used by the reducer to count.
         */
        val message = matcher.group(1)
        context.write(new Text(message), one)
      }
    }
  }

  /**
   * Reducer: Used to count the strings groupwise.
   */
  class reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  /**
   * Driver
   */
  def main(args: Array[String]): Unit = {
    import org.apache.hadoop.fs.FileSystem
    val logger = CreateLogger(classOf[mr3.type])
    
    /**
     * Extract the necessary parameters from the config file (application.conf)
     */
    val configfile = ConfigFactory.load()
    logger.info("Loading config values")
    val inp_path = configfile.getString("common.input_path")
    val out_path = configfile.getString("mr3.output_path")
    logger.info("Loaded config values")

    /**
     * Setup the configuration to be used for the job.
     */
    val configuration = new Configuration
    
    /**
     * Logic to delete temporary output folder if exists.
     */
    val fs = FileSystem.get(configuration)
    logger.info("Checking for existing output data.")
    if (fs.exists(new Path(out_path))) fs.delete(new Path(out_path), true)

    /**
     * Setup the job with the mapper and reducer.
     */
    logger.info("Starting the job.")
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[mapper])
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