/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package movements

import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object StopDetectionJob {

  val log = LoggerFactory.getLogger(StopDetectionJob.getClass)

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("You must pass the arguments: " +
        "<src file> <max points per partition> <eps> <min points per partition>")
      System.exit(1)
    }

    log.info("Parse arguments of the function")
    val (src, maxPointsPerPartition, eps, minPoints) =
      (args(0), args(1).toInt, args(2).toFloat, args(3).toInt)

    log.info("Create Spark Context")
    val conf = new SparkConf()
    conf.setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition)")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)

    log.info("Parse Input File to StopPoint class instances")
    val data = sc.textFile(src)
    val parsedData = data.map(s => DetectedPoint(s.split(','))).cache()

    log.debug("Filter Moves to obtain stops only")
    val detectedStops = filterMovements(parsedData)

    // TODO: remove, this print is just for debugging
    detectedStops.foreach(detectedPoint => println(detectedPoint.toString()))

    log.debug("Cluster Points")
    val model = DBSCAN.train(
      detectedStops,
      eps,
      minPoints,
      maxPointsPerPartition)

    val clusteredData = model.labeledPoints.map(p => s"${p.x},${p.y},${p.cluster}")

    // TODO: remove, this print is just for debugging
    clusteredData.foreach(clusteredPoint=> println(clusteredPoint.toString()))
    
    log.info("Stopping Spark Context...")
    sc.stop()

  }

  /**
    * This function filters all DetectedPoints and
    * return Vector with (0)->Latitude and (1)->Longitude
    */
  def filterMovements(detectedPoints: RDD[DetectedPoint]): RDD[Vector] = {
    detectedPoints
      .filter(isStop)
      .map(detectedPoint => Vectors.dense(detectedPoint.lat, detectedPoint.long))
  }

  def isStop(detectedPoint: DetectedPoint): Boolean = {
    // TODO: Write stop function
    true
  }

}