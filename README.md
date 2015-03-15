# DBSCAN on Spark

### Overview

This is an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) 
on top of [Apache Spark](http://spark.apache.org/). It is based on the parem from He, Yaobin, et al.
["MR-DBSCAN: an efficient parallel density-based clustering algorithm using mapreduce"](http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6121313). 

### Getting DBSCAN on Spark

DBSCAN on Spark is published to [bintray](https://bintray.com/). If you use SBT you
can include SBT in your application adding the following to your build.sbt:

```
resolvers += "bintray/irvingc" at "https://bintray.com/irvingc/maven"

libraryDependencies += "com.irvingc.spark" %% "dbscan" % "0.1.0"
```

If you use Maven or Ivy you can use a similar resolver, but you just
need to account for the scala version (the example is for Scala 2.10):

```
...

	<repositories>
		<repository>
			<id>dbscan-on-spark-repo</id>
			<name>Repo for DBSCAN on Spark</name>
			<url>http://dl.bintray.com/irvingc/maven</url>
		</repository>
	</repositories>
...

	<dependency>
		<groupId>com.irvingc.spark</groupId>
		<artifactId>dbscan_2.10</artifactId>
		<version>0.1.0</version>
	</dependency>


```
DBSCAN on Spark is published against Scala 2.10.


### Example usage 

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

### License

DBSCAN on Spark is available under the Apache 2.0 license. 
See the [LICENSE](LICENSE) file for details.


### Credits

DBSCAN on Spark is maintained by Irving Cordova (irving@irvingc.com).





