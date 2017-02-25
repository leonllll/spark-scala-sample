package com.leon.code.sample.spark.scala.io

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
/**
 * sample of reading and writing in Spark
 * tested in Spark 1.4.1
 * @author leon
 */
object ReadWriteSample {

  def readWholeTextFiles() {
    val conf = new SparkConf().setAppName("spark-scala-test").setMaster("local")
    //what the `wholeTextFiles` return is a Pair RDD
		//1. read the files at a local path 
    //2. convert the Pair RDD to a RDD
    //3. print every filename and content
    var path = "file://" + this.getClass.getClassLoader.getResource("data/people-1.txt").getPath;
    val sc = new SparkContext(conf)
    sc.wholeTextFiles(path)
      .map(t => t._2)
      .collect
      .foreach { x => println(x) }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark-rdd-test")
//    conf.set("spark.executor.extraJavaOptions",	
//        "-XX:+UseCompressedOops -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
//    conf.set("spark.executor.instances", "2");
//    conf.set("spark.executor.memory","512m");
//    conf.set("mapreduce.input.fileinputformat.split.minsize", "64m");
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://ice5:9000/user/leon/wiki-edit-history-topic-2016-09-06-23.1473175558976");
    println(rdd.setName("test").persist.collect().size)
    rdd.partitions.foreach { x => println(x) }
    //sc.getRDDStorageInfo.foreach { x => println(x) };
    Thread.sleep(60000);
  }

}
