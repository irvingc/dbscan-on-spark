# DBSCAN on Spark

This is an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) 
on top of [Apache Spark](http://spark.apache.org/). It is based on the parem from He, Yaobin, et al.
["MR-DBSCAN: an efficient parallel density-based clustering algorithm using mapreduce"](http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6121313). 

## Example usage 

```scala
import org.apache.spark.mllib.clustering.dbscan.DBSCAN

object DBSCANSample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DBSCAN Sample")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val labeled = DBSCAN.fit(eps = 0.3F, minPoints = 10, data = parsedData)

    labeled.saveAsTextFile(args(1))

  }
}
```

You can optionally set the parallelism to the job depending on the number of cores in your cluster.

## Credits

DBSCAN on Spark is maintained by irving (irving@irvingc.com)




