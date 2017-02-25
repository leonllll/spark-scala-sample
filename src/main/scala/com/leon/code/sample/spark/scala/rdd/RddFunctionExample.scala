package com.leon.code.sample.spark.scala.rdd

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.collection.mutable.ListBuffer;
/**
 * the RDD Transformations and Actions by example
 * tested in Spark 2.1.0
 * @author leon
 */
object RddFunctionExample {
  
  def basicRddTransformations(sc: SparkContext) {
    
    val rdd = sc.parallelize(List(3, 1, 3, 2), 2)
    rdd.cache()
    
    // 1. map
    val resultOfMap = rdd.map(x => x + 1).collect
    println("map: " + resultOfMap.mkString(", "))
    
    // 2. filter
    val resultOfFilter = rdd.filter(x => x != 1).collect
    println("filter: " + resultOfFilter.mkString(", "))
    
    // 3. flatMap
    val resultOfFlatMap = rdd.flatMap(x => x.to(3)).collect
    println("filter: " + resultOfFlatMap.mkString(", "))
    
    // 4. mapPartitions
    // get the minimum record in each partition
    // sort the records and take the first one
    val resultOfMapPartitions = rdd.mapPartitions(
        iter => iter.toList.sorted.take(1).iterator 
    ).collect
    println("mapPartitions: " + resultOfMapPartitions.mkString(", "))
    
    // 5. mapPartitionsWithIndex
    val resultOfMapPartitionsWithIndex = rdd.mapPartitionsWithIndex(
        (idx, iter) => { if(idx == 0) iter.toList.take(1).iterator else iter } 
    ).collect
    println("mapPartitionsWithIndex: " + resultOfMapPartitionsWithIndex.mkString(", "))
    
    // 6. sample
    val resultOfSample = rdd.sample(false, 0.5).collect
    println("sample: " + resultOfSample.mkString(", "))
    
    // 7. distinct
    val resultOfDistinct = rdd.distinct.collect
    println("distinct: " + resultOfDistinct.mkString(", "))
    
    // 8. pipe
    // Takes the RDD data of each partition and sends it via stdin to a shell-command.
    // The resulting output of the command is captured and returned as a RDD of string values.
    // The 'head -n' command prints the first n lines of a file
    val resultOfPipe = rdd.pipe("head -n 1").collect
    println("pipe: " + resultOfPipe.mkString(", "))
    
    // 9. glom
    val resultOfGlom = rdd.glom.collect
    println("glom: ")
    resultOfGlom.map{ x => println(x.mkString(", "))}
    
    // 10. groupBy
    val resultOfGroup = rdd.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect
    println("groupBy: ")
    resultOfGroup.map{ x => println(x._1 + ", {" + x._2.mkString(", ") + "}")}
  }
  
  def twoRddsTransformations(sc: SparkContext) {
    val rdd = sc.parallelize(List(1, 2, 3)).cache
    val other = sc.parallelize(List(3, 4, 5)).cache
    
    // 1. union
    val resultOfUnion = rdd.union(other).collect
    println("union: " + resultOfUnion.mkString(", "))
    
    // 2. intersection
    val resultOfIntersection = rdd.intersection(other).collect
    println("intersection: " + resultOfIntersection.mkString(", "))
    
    // 3. cartesian
    val resultOfCartesian = rdd.cartesian(other).collect
    println("cartesian: " + resultOfCartesian.mkString(", "));
    
    // 4. subtract
    val resultOfSubtract = rdd.subtract(other).collect
    println("subtract: " + resultOfSubtract.mkString(", "));
    
    // 5. zip
    val resultOfZip = rdd.zip(other).collect
    println("zip: " + resultOfZip.mkString(", "));
  }
  
  def pairRDDTransformations(sc: SparkContext) {
    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6))).cache
    
    // 1. groupByKey
    val resultOfGroupByKey = rdd.groupByKey.collectAsMap
    println("groupByKey: ")
    resultOfGroupByKey.map{ x => println("(" + x._1 + ", [" + x._2.mkString(", ") + "])" )}

    // 2. reduceByKey
    val resultOfReduceByKey = rdd.reduceByKey( (x,y)=>x+y).collect
    println("reduceByKey: " + resultOfReduceByKey.mkString(", "))
    
    // 3. aggregateByKey
    //get the maximum 
    val resultOfAggregateByKey1 = rdd.aggregateByKey(-1)(
            (max, value) => math.max(max, value), // in the accumulator, keep the number which is bigger
            (max1, max2) => math.max(max1, max2)) // get the max number among all the accumulators
            .collect
    println("aggregateByKey-1: " + resultOfAggregateByKey1.mkString(", ")) 
    
    // get the average
    val resultOfAggregateByKey2 = rdd.aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1), // record the sum of the numbers, and the amount of the numbers
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) // accumulate the values
        .map({case (key, acc) =>(key, acc._1.toDouble / acc._2)}) // calculate the average
        .collect
    println("aggregateByKey-2: " + resultOfAggregateByKey2.mkString(", "))
    
    // 4. sortByKey
    val resultOfSortByKey = rdd.sortByKey(false).collect
    println("sortByKey: " + resultOfSortByKey.mkString(", "))
    
    // 5. combineByKey
    val resultOfCombineByKey = rdd.combineByKey(
        v => (v, 1), 
        (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .collect
    println("combineByKey: " + resultOfCombineByKey.mkString(", "))
    
    // 6. mapValues
    val resultOfMapValues = rdd.mapValues( x => x+1).collect
    println("mapValues: " + resultOfMapValues.mkString(", "))
    
    // 7. flatMapValues
    val resultOfFlatMapValues = rdd.flatMapValues(x => (x to 5)).collect
    println("flatMapValues: " + resultOfFlatMapValues.mkString(", "))
    
    // 8. keys
    val resultOfKeys = rdd.keys.collect
    println("keys: " + resultOfKeys.mkString(", "))
    
    // 9. values
    val resultOfValues = rdd.values.collect
    println("values: " + resultOfValues.mkString(", "))

  }
  
  def twoPairRddsTransformations(sc: SparkContext) {
    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6))).cache
    val other = sc.parallelize(List((3, 9))).cache
    
    // 1. join
    val resultOfJoin = rdd.join(other).collect
    println("join: " + resultOfJoin.mkString(", "))
    
    // 2. rightOuterJoin
    val resultOfRightOuterJoin = rdd.rightOuterJoin(other).collect
    println("rightOuterJoin: " + resultOfRightOuterJoin.mkString(", "))
    
    // 3. leftOuterJoin
    val resultOfLeftOuterJoin = rdd.leftOuterJoin(other).collect
    println("leftOuterJoin: " + resultOfLeftOuterJoin.mkString(", "))
    
    // 4. cogroup
    val resultOfCogroup = rdd.cogroup(other).collect
    println("cogroup: ")
    resultOfCogroup.map{ 
      case (key, group) => 
        println("(" + key + ", (" + group._1.toList + ", " + group._2.toList + "))" )
    }
    
    // 5. subtractByKey
    val resultOfSubtractByKey = rdd.subtractByKey(other).collect
    println("subtractByKey: " + resultOfSubtractByKey.mkString(", "))
    
  }
  
  def basicRddActions(sc: SparkContext) {
   val rdd = sc.parallelize(List(1, 2, 3, 3)).cache
   
   // 1. reduce
   println("reduce: " + rdd.reduce(_ + _));
   
   // 2. collect
   println("collect: " + rdd.collect.mkString(", "));
   
   // 3. count
   println("count: " + rdd.count)
   
   // 4. first
   println("first: " + rdd.first)
   
   // 5. take
   println("take: " + rdd.take(2).mkString(", "))
   
   // 6. takeSmple
   println("takeSample: " + rdd.takeSample(false, 1).mkString(", "))
   
   // 7. takeOrdered
   println("takeOrdered: " + rdd.takeOrdered(2).mkString(", "))
   
   // 8. countByValue
   println("countByValue: " + rdd.countByValue.mkString(", "))
   
   // 9. top
   println("top: " + rdd.top(2).mkString(", "))
   
   // 10. fold
   println("fold: " + rdd.fold(0)(_ + _))
   
   // 11. aggregate
   println("aggregate: " + rdd.aggregate((0,0))(
       (acc, v) => (acc._1 + v,acc._2 + 1),
       (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)))      
   
  }
  
  def PairRddActions(sc: SparkContext) {
    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6))).cache
    
    // 1. countByKey
    println("countByKey: " + rdd.countByKey)
    
    // 2. collectAsMap
    println("collectAsMap: " + rdd.collectAsMap)
    
    // 3. lookup
    println("lookup: " + rdd.lookup(3).mkString(", "))
  }
  
  def main(args: Array[String]) {
   
    val conf = new SparkConf().setAppName("spark-rdd-test").setMaster("local")
    val sc = new SparkContext(conf)
    
    basicRddTransformations(sc)
    twoRddsTransformations(sc) 
    pairRDDTransformations(sc)
    twoPairRddsTransformations(sc)
    basicRddActions(sc)
    PairRddActions(sc)
  }

}
