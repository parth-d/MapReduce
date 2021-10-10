package com.parth.scala

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

object mr4 {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      //To get absolute value of time and divide
      // To filter using pattern_match
      val pattern = Pattern.compile("(INFO|ERROR|WARN|DEBUG).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        val group = matcher.group(1)
        val message = matcher.group(2)
        val pattern_match = Pattern.compile(".*")
        // to add pattern from config file
//        val pattern_match = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
        val pattern_matcher = pattern_match.matcher(message)
        if (pattern_matcher.find()){
          context.write(new Text(group), new IntWritable(message.length))
        }
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val maxv = values.asScala.foldLeft(0){(max, curr) => math.max(max, curr.get)}
      context.write(key, new IntWritable(maxv))
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    // Add config file to store global regex
    val pattern_match = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
    configuration.set("pattern_match", pattern_match)
    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)), true)
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}