package com.leon.scala.test

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
/**
 * @author leon
 */
object App {

  def readWholeTextFiles() {
    val conf = new SparkConf().setAppName("scala-test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.wholeTextFiles("file:///Users/leon/Documents/test/")
      .map(t => t._2)
      .collect
      .foreach { x => println(x) }
  }

  def main(args: Array[String]) {
    readWholeTextFiles()
  }

}
