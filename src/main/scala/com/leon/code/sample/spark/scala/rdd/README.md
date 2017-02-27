# the RDD Functions by example

## Basic RDD transformations 
example of running basic transformations on an RDD containing {3, 1, 3, 2} with 2 Partitions
```
rdd = sparkContext.parallelize(Array(3, 1, 3, 2), 2)
```
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
map(func) | Return a new distributed dataset formed by passing each element of the source through a function func. | No | rdd.map(x => x + 1) | {4, 2, 4, 3} |
filter(func) | Return a new dataset formed by selecting those elements of the source on which func returns true. | No | rdd.filter(x => x != 1) | {3, 3, 2} |
flatMap(func) | Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item). | No | rdd.flatMap(x => x.to(3)) | {3, 1, 2, 3, 3, 2, 3} |
mapPartitions(func)	| Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type `Iterator<T> => Iterator<U>` when running on an RDD of type T. |No|rdd.mapPartitions(iter => iter.toList.sorted .take(1).iterator)  |  {1, 2} |
mapPartitionsWithIndex(func) | Similar to mapPartitions, but also provides func with an integer value representing the index of the partition, so func must be of type `(Int, Iterator<T>) => Iterator<U>` when running on an RDD of type T. | No | rdd. mapPartitionsWithIndex( (idx, iter) => { if(idx == 0) iter.toList.take(1).iterator else iter } ) | {3, 3, 2} | 
sample(withReplacement, fraction, seed) | Sample a fraction _fraction_ of the data, with or without replacement, using a given random number generator seed.  | No | rdd.sample(false, 0.5).collect | Nondeterministic e.g. {3, 3}|
distinct([numTasks])) |Return a new dataset that contains the distinct elements of the source dataset. | Yes | rdd.distinct() | {2, 1, 3} |
pipe(command, [envVars]) | Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings |No|rdd.pipe("head -n 1") | {3, 3} |
glom | Assembles an array that contains all elements of the partition and embeds it in an RDD. Each returned array contains the contents of one partition | No | rdd.glom | {{3, 1}, {3,  2} } |
groupBy(func) | Return an RDD of grouped items. Each group consists of a key and a sequence of elements mapping to that key. The ordering of elements within each group is not guaranteed, and may even differ each time the resulting RDD is evaluated | Yes | rdd.groupBy(x => { if (x % 2 == 0) "even" else "odd" }) | {(even, {2}), (odd, {3, 1, 3})} |
sortBy(func, ascending) | This function sorts the input RDD's data and stores it in a new RDD. The first parameter requires you to specify a function which  maps the input data into the key that you want to sortBy. The second parameter (optional) specifies whether you want the data to be sorted in ascending or descending order | Yes | rdd.sortBy(x => x, false) | {3, 3, 2, 1} |

## Two RDDs transformations 
example of running two-RDD transformations on RDDs containing {1, 2, 3}  and {3, 4 ,5}:
```
rdd = sc.parallelize(List(1, 2, 3)).cache
other = sc.parallelize(List(3, 4, 5)).cache
```
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
union(otherDataset) | Return a new dataset that contains the union of the elements in the source dataset and the argument. | No | rdd.union(other) | {1, 2, 3, 3, 4, 5} | 
intersection(otherDataset) | Return a new RDD that contains the intersection of elements in the source dataset and the argument. | Yes | rdd.intersection(other) | {3} |
cartesian(otherDataset) | When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). | Yes | rdd.cartesian(other) | {(1,3), (1,4), (1,5), (2,3), (2,4), (2,5), (3,3), (3,4), (3,5)} | 
subtract(otherDataset) | Return a new dataset that contains the elements in the source dataset and not in the argument | Yes | rdd.subtract(other) | {1, 2} |
zip(otherDataset) | Zips this RDD with another one, returning key-value pairs with the first element in each RDD, second element in each RDD, etc. Assumes that the two RDDs have the *same number of partitions* and the *same number of elements in each partition* (e.g. one was made through a map on the other). | No | rdd.zip(other) | {(1,3), (2,4), (3,5)} |
## Pair RDD transformations 
example of running pair-RDD transformations on an RDD containing {(1, 2), (3, 4), (3, 6)}:
```
rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
```  
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
groupByKey([numTasks])	 | When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.  Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks. | Yes | rdd.groupByKey | {(1, [2]), (3, [4, 6])}|
reduceByKey(func, [numTasks]) | When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument. | Yes | rdd.reduceByKey( (x,y)=>x+y) | {(1,2), (3,10)} |
aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])	| When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument. | Yes | rdd.aggregateByKey(-1)(  (max, value) => math.max(max, value), (max1, max2) => math.max(max1, max2)) | {(1,2), (3,6)} |
sortByKey([ascending], [numTasks]) | When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument. | Yes | rdd.sortByKey(false) | {(3,4), (3,6), (1,2)} |
combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner) | Combine values with the same key using a different result type. | Yes | rdd.combineByKey( v => (v, 1),  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) | (1,(2,1)), (3,(10,2)) |
mapValues(func) | Apply a function to each value of a pair RDD without changing the key | No | rdd.mapValues(x => x+1) | {(1,3), (3,5), (3,7)} |
flatMapValues(func) | Apply a function that returns an iterator to each value of a pair RDD, and for each element returned, produce a key/value entry with the old key. Often used for tokenization. | No | rdd.flatMapValues(x => (x to 5) | {(1,2), (1,3), (1,4), (1,5), (3,4), (3,5)} |
keys() | Return an RDD of just the keys. | No | rdd.keys() | {1, 3, 3} |
values() | Return an RDD of just the values. | No | rdd.values() | {2, 4, 6} |
## Two Pair RDDs transformations 
example of running two-pair-RDD transformations on RDDs containing {(1, 2), (3, 4), (3, 6)}  and {(3, 9)}:
```
rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
other = sc.parallelize(List((3, 9)))
```
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
| join(otherDataset, [numTasks]) | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin. | Yes | rdd.join(other) | {(3,(4,9)), (3,(6,9))} |
rightOuterJoin(otherDataset) | Perform a join between two RDDs where the key must be present in the first RDD. | Yes | rdd.rightOuterJoin(other) | {(3,(Some(4),9)), (3,(Some(6),9))} |
leftOuterJoin(otherDataset) | Perform a join between two RDDs where the key must be present in the other RDD. | Yes | rdd.leftOuterJoin(other) | {(1, (2,None)), (3, (4,Some(9))), (3, (6,Some(9)))} |
cogroup(otherDataset, [numTasks]) | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith. | No | rdd.cogroup(other) | {(1,([2],[])), (3, ([4, 6],[9]))} |
subtractByKey(otherDataset) | Remove elements with a key present in the other RDD. | Yes | rdd.subtractByKey(other) | {(1, 2)} |
 
## Basic RDD actions 
example of running basic RDD actions on an RDD containing {1, 2, 3, 3}:
```
rdd = sc.parallelize(List(1, 2, 3, 3))
```
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
reduce(func) | Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. | Yes | rdd.reduce(_ + _) |  9 |
collect() | Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. | No | rdd.collect | {1, 2, 3, 3} |
count() | Return the number of elements in the dataset. | No | rdd.count | 4 |	 
first() | Return the first element of the dataset (similar to take(1)). | No | rdd.first | 1 |
take(n) | Return an array with the first n elements of the dataset. | No | rdd.take(2) | {1, 2} |
takeSample( withReplacement, num, [seed])	| Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed. | No | rdd.takeSample (false, 1) | Nondeterministic |
takeOrdered(n, [ordering])	| Return the first n elements of the RDD using either their natural order or a custom comparator. | Yes | rdd.takeOrdered(2) | {1, 2} |
countByValue() | Number of times each element occurs in the RDD | Yes | rdd.countByValue | {1 -> 1, 3 -> 2, 2 -> 1} |
top(n) | Return the top num elements the RDD | Yes | rdd.top(2) | {3, 3} | 
fold(zeroValue)(func) | Same as reduce() but with the provided zero value. | Yes | rdd.fold(0)(_ + _) | 9 |
aggregate(zeroValue) (seqOp, combOp) | Similar to reduce() but used to return a different type. | Yes | rdd.aggregate((0,0))( (acc, v) => (acc._1 + v,acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) | (9,4) |

## Pair RDD actions 
example of running pair-RDD actions on an RDD containing {(1, 2), (3, 4), (3, 6)} :
```
rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
```
Name| Meaning | Shuffle | Example | Result |
---|---|---|---|---|
countByKey() | Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. | Yes | rdd.countByKey | {1 -> 1, 3 -> 2} |
collectAsMap | Similar to collect, but works on key-value RDDs and converts them into Scala maps to preserve their key-value structure | No | rdd.collectAsMap | {1 -> 2, 3 -> 6} |
lookup | Return all values associated with the rdd.lookup(3) provided key | No | rdd.lookup(3) | {4, 6} |
## Other actions
Name| Meaning | 
---|---|
saveAsTextFile(path) | Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. |
saveAsSequenceFile(path)  | (Java and Scala) Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). |
saveAsObjectFile(path) | (Java and Scala) Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile(). |
foreach | Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems |

### Reference
1. [Learning Spark, Lightning-Fast Big Data Analysis](http://shop.oreilly.com/product/0636920028512.do)
2. [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html#accumulators)
3. [The RDD API By Example](http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#glom)