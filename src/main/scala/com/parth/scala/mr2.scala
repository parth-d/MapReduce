package com.parth.scala

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
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.collection.JavaConverters.*
import scala.collection.mutable.ListBuffer

object mr2 {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val intervals = context.getConfiguration.get("Interval").toInt
      //To get absolute value of time and divide
      // To filter using pattern_match
      val pattern = Pattern.compile("(.*) \\[.*\\] (ERROR).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){

        val pattern_match = Pattern.compile(".*")
        //to add pattern from config file
//        val pattern_match = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
        val pattern_matcher = pattern_match.matcher(matcher.group(3))
        if (pattern_matcher.find()){
          val timex = new SimpleDateFormat("HH:mm:ss.SSS").parse(matcher.group(1))
          val group_number = new SimpleDateFormat("mmss").format(timex).toInt/intervals
          val seconds = TimeUnit.MILLISECONDS.toSeconds(timex.getTime)
          val formatted = new SimpleDateFormat("HH:mm:ss").format(timex)
//          val message : String = matcher.group(2)
          val message : String = "ERROR"
          context.write(new Text(group_number.toString), one)
        }
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
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

  class InverseMapper extends Mapper[Object, Text, IntWritable, Text]{
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit ={
//      val values = value.toString.split("\\s+")
//      context.write(new Text(values(1)), new IntWritable(values(0).toInt))
      val pattern = Pattern.compile("(.*)\\s+(.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find())
        context.write(new IntWritable(matcher.group(2).toInt), new Text(matcher.group(1)))
    }
  }

  class SortReducer extends Reducer[IntWritable, Text, IntWritable, Text]{
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {
      values.forEach(text => {
        context.write(key, text)
      })
    }
  }



  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    // Add config file to store global regex
    val pattern_match = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
    configuration.set("Interval", args(3))
    configuration.set("pattern_match", pattern_match)
    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)), true)
    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) {

      if (fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)), true)
      val sortjob = Job.getInstance()
      sortjob.setJarByClass(this.getClass)
      sortjob.setMapperClass(classOf[InverseMapper])
      sortjob.setReducerClass(classOf[SortReducer])
      sortjob.setSortComparatorClass(classOf[comparator])
      sortjob.setOutputKeyClass(classOf[IntWritable])
      sortjob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(sortjob, new Path(args(1)))
      FileOutputFormat.setOutputPath(sortjob, new Path(args(2)))
      System.exit(if (sortjob.waitForCompletion(true)) 0 else 1)
    }
  }
}