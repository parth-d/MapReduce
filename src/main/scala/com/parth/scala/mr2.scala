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
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.collection.JavaConverters.*
import scala.collection.mutable.ListBuffer

object mr2 {

  class Mapper1 extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      // aadd config file
      // add logic for minumum grp
      val intervals = context.getConfiguration.get("Interval").toInt
      val imported_pattern = context.getConfiguration.get("pattern_match")

      val pattern = Pattern.compile("(.*) \\[.*\\] (ERROR).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        val pattern_match = Pattern.compile(imported_pattern)
        val pattern_matcher = pattern_match.matcher(matcher.group(3))
        if (pattern_matcher.find()){
          val timex = new SimpleDateFormat("HH:mm:ss.SSS").parse(matcher.group(1))
          val group_number = new SimpleDateFormat("mmss").format(timex).toInt/intervals
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
        context.write(new IntWritable(matcher.group(2).toInt), new Text(matcher.group(1)))
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
    val configfile = ConfigFactory.load()
    val configuration = new Configuration
    val pattern_match = configfile.getString("main.pattern")
    configuration.set("min_group", "10000")
    configuration.set("Interval", args(3))
    configuration.set("pattern_match", pattern_match)
    import org.apache.hadoop.fs.FileSystem
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(args(1)))) fs.delete(new Path(args(1)), true)
    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Mapper1])
    job.setCombinerClass(classOf[Reducer1])
    job.setReducerClass(classOf[Reducer1])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) {

      if (fs.exists(new Path(args(2)))) fs.delete(new Path(args(2)), true)
      val sortjob = Job.getInstance()
      sortjob.setJarByClass(this.getClass)
      sortjob.setMapperClass(classOf[Mapper2])
      sortjob.setReducerClass(classOf[Reducer2])
      sortjob.setSortComparatorClass(classOf[comparator])
      sortjob.setOutputKeyClass(classOf[IntWritable])
      sortjob.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(sortjob, new Path(args(1)))
      FileOutputFormat.setOutputPath(sortjob, new Path(args(2)))
      System.exit(if (sortjob.waitForCompletion(true)) 0 else 1)
    }
  }
}