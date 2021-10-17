package com.parth.scala

import com.typesafe.config.ConfigFactory
import org.apache.commons.beanutils.converters.DateTimeConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, Writable, WritableComparable, WritableComparator}
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
 * This class is made to execute the second part of the functionality which shows the distribution of different type of messages across predefined time intervals (application.conf).
 * First mapreduce job (Filter + count):
 *  The logic used is to first parse each line and extract the intervals in the matcher. Now, the time is parsed and the log level is parsed.
 *  The string is then matched with the regex pattern defined in the application.conf file and if it matches, the mapper is instructed to write to context the corresponding time interval, log level and 'one' which is an intWritable.
 *  Here, the interval number is a value obtained by an algorithm which produces appropriate groups based on the time interval. This logic can be seen in line 67.
 *  The reducer sums the matched values for each group (time interval, log level).
 * Second mapreduce job (Sort):
 *  The logic used is to split each line to extract the interval number and convert it back into mm:ss format to be put in the output. This splitting logic is implemented by the mapper whereas the reducer does not perform any special operation.
 *
 * The final output is in the following format:
 *    Interval start time (mm:ss)  |  Log Level  |  Number of matching strings
 */

object mr2 {

  class Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val intervals = context.getConfiguration.get("Interval").toInt
      val imported_pattern = context.getConfiguration.get("pattern_match")

      val pattern = Pattern.compile("(.*) \\[.*\\] (ERROR).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        val pattern_match = Pattern.compile(imported_pattern)
        val pattern_matcher = pattern_match.matcher(matcher.group(3))
        if (pattern_matcher.find()){
          val timex = new SimpleDateFormat("HH:mm:ss.SSS").parse(matcher.group(1))
          val group_number = timex.getTime.toInt/(1000*intervals)
          context.write(new Text(group_number.toString), one)
        }
      }
    }
  }

  class Reducer1 extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  class comparator extends WritableComparator(classOf[IntWritable], true){
    @SuppressWarnings(Array("rawtypes"))
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
      val k1 = a.asInstanceOf[IntWritable]
      val k2 = b.asInstanceOf[IntWritable]
      -1 * k1.compareTo(k2)
    }
  }

  class Mapper2 extends Mapper[Object, Text, IntWritable, Text]{
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit ={
      val pattern = Pattern.compile("(.*)\\s+(.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find())
        val intervals = context.getConfiguration.get("Interval").toInt
        val date = new Date(matcher.group(1).toLong * intervals * 1000L)
        val time = new SimpleDateFormat("mm:ss").format(date)
        context.write(new IntWritable(matcher.group(2).toInt), new Text(time.toString))
    }
  }

  class Reducer2 extends Reducer[IntWritable, Text, IntWritable, Text]{
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {
      values.forEach(text => {
        context.write(key, text)
      })
    }
  }



  def main(args: Array[String]): Unit = {
    val configuration = new Configuration

    val configfile = ConfigFactory.load()
    val pattern_match = configfile.getString("common.pattern")
    val inp_path = configfile.getString("common.input_path")
    val temp_path = configfile.getString("mr2.temp_path")
    val out_path = configfile.getString("mr2.output_path")
    val interval = configfile.getString("common.time_interval")

    configuration.set("Interval", interval)
    configuration.set("pattern_match", pattern_match)
    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(temp_path))) fs.delete(new Path(temp_path), true)
    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Mapper1])
    job.setCombinerClass(classOf[Reducer1])
    job.setReducerClass(classOf[Reducer1])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(inp_path))
    FileOutputFormat.setOutputPath(job, new Path(temp_path))
    if (job.waitForCompletion(true)) {

      if (fs.exists(new Path(out_path))) fs.delete(new Path(out_path), true)
      val config2 = new Configuration()
      config2.set("Interval", interval)
//      config2.set("mapred.textoutputformat.separator", ",")
      val sortjob = Job.getInstance(config2, "Sort")
      sortjob.setJarByClass(this.getClass)
      sortjob.setMapperClass(classOf[Mapper2])
      sortjob.setReducerClass(classOf[Reducer2])
      sortjob.setSortComparatorClass(classOf[comparator])
      sortjob.setOutputKeyClass(classOf[IntWritable])
      sortjob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(sortjob, new Path(temp_path))
      FileOutputFormat.setOutputPath(sortjob, new Path(out_path))
      System.exit(if (sortjob.waitForCompletion(true)) 0 else 1)
    }
  }
}