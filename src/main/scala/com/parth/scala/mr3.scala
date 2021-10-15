package com.parth.scala

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.util.regex.Pattern
import scala.collection.JavaConverters.*
import java.lang.Iterable
object mr3 {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile("(INFO|ERROR|WARN|DEBUG).*- (.*)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()){
        val message = matcher.group(1)
        context.write(new Text(message), one)
      }
    }
  }

  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }


  def main(args: Array[String]): Unit = {
    import org.apache.hadoop.fs.FileSystem

    val configfile = ConfigFactory.load()
    val inp_path = configfile.getString("common.input_path")
    val out_path = configfile.getString("mr3.output_path")
    val configuration = new Configuration
    val fs = FileSystem.get(configuration)
    if (fs.exists(new Path(out_path))) fs.delete(new Path(out_path), true)
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(inp_path))
    FileOutputFormat.setOutputPath(job, new Path(out_path))

    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}