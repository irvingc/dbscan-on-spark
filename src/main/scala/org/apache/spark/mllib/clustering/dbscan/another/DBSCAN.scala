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
package org.apache.spark.mllib.clustering.dbscan.another

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.Logging
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.clustering.dbscan.another.DBSCANLabeledPoint.Flag
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object DBSCAN {

}

class DBSCAN(
  eps: Double,
  minPoints: Int,
  maxPointsPerPartition: Int)
  extends Serializable with Logging {

  type GlobalClusterId = (Int, Int)
  type MarginSet = (DBSCANBox, DBSCANBox, DBSCANBox)
  type PartitionPointPair = (Int, DBSCANLabeledPoint)

  val gridSize = 2 * eps

  def train(vectors: RDD[Vector]): DBSCANModel = {

    logInfo(s"Maximum number of points per partition: $maxPointsPerPartition")

    val points: RDD[DBSCANPoint] =
      vectors.map(v => DBSCANPoint(v))

    // Split the space into a grid 
    val grids: RDD[(DBSCANBox, Int)] =
      points.map(p => (toGridBox(p), 1))

    // Count the points per grid
    val gridsWithCount: Set[(DBSCANBox, Int)] =
      grids
        .aggregateByKey(0)(_ + _, _ + _)
        .collect()
        .toSet

    // Find the best partitions based on the given grid
    val localPartitionsWithCount: List[(DBSCANBox, Int)] =
      EvenSplitPartitioner.findPartitions(gridsWithCount, maxPointsPerPartition, gridSize)

    val numOfPartitions = localPartitionsWithCount.size

    logInfo("The following partitions were found:")
    localPartitionsWithCount.foreach({
      case (r, c) => {
        val width = r.x2 - r.x
        val height = r.y2 - r.y
        logInfo(s"Rectangle((${r.x},${r.y}),$width,$height,edgecolor=colors[0],fc=colors[0]), #$c")
      }
    })

    // grow partitions to include margins
    val localPartitionsWithMarginsAndIndex: List[(MarginSet, Int)] =
      localPartitionsWithCount
        .map({
          case (p, _) => (p.contract(eps), p, p.contract(-eps))
        })
        .zipWithIndex

    val partitions = vectors.context.broadcast(localPartitionsWithMarginsAndIndex)

    // duplicate points that belong to several partitions
    val duplicated: RDD[(Int, DBSCANPoint)] =
      points
        .flatMap(point => {
          partitions.value
            .filter({ case ((inner, main, outer), _) => outer.contains(point) })
            .map({ case (_, id) => (id, point) })
        })
        
    
    // run dbscan on each partition
    val localEps = eps
    val localMinPoints = minPoints

    val clustered: RDD[(Int, DBSCANLabeledPoint)] =
      duplicated
        .groupByKey(numOfPartitions)
        .flatMapValues(points =>
          new LocalDBSCANArchery(localEps, localMinPoints).fit(points))
        .cache()

    // get a list of all identified clusters
    val localListOfClusters: Array[GlobalClusterId] =
      clustered
        .filter({
          case (_, point) => point.flag != DBSCANLabeledPoint.Flag.Noise
        })
        .mapValues(_.cluster)
        .distinct()
        .collect()

    logDebug("LocalList")
    localListOfClusters.foreach(x => logDebug(x.toString()))
    logDebug("Ends LocalList")

    // create a list of clusters that are the same
    // based on a point having been assigned multiple clusters 
    val localIntersections: Array[(GlobalClusterId, GlobalClusterId)] =
      clustered
        .filter({
          case (id, point) => {
            point.flag != Flag.Noise && isInsideMargin(point, partitions.value)
          }
        })
        .map(p => toPointWithMainPartition(p, partitions.value))
        .groupByKey(numOfPartitions)
        .flatMapValues(points => findClusterAliases(points))
        .values
        .collect()

    val adjacencies = AdjacencyGraph(localIntersections)

    logDebug("Intersections")
    localIntersections.foreach(x => logDebug(x.toString()))
    logDebug("Ends Intersections")

    // assign an id to each of the identified global clusters
    // using the previously found correspondence list
    // to assign the same id to clusters that are the same
    val (_, localGlobalClusterIds) =
      localListOfClusters
        .foldLeft((1, Map[GlobalClusterId, Int]()))({
          case ((id, map), clusterId) => {
            if (!map.contains(clusterId)) {

              (id + 1,
                map + (clusterId -> id) ++
                adjacencies.getAdjacent(clusterId).map(a => (a, id)).toMap)

            } else {
              (id, map)
            }
          }
        })

    logDebug("Global Cluster IDs")
    localGlobalClusterIds.foreach(id => logDebug(id.toString()))
    val uniqueClusters = localGlobalClusterIds.values.toSet.size
    logInfo(s"Total Clusters: ${localGlobalClusterIds.size}, Unique: $uniqueClusters")

    val globalClusterIds = vectors.context.broadcast(localGlobalClusterIds)

    // remove the duplicated points, and assign them the global id
    val labeledPoints = clustered
      .map(toPointWithMainPartition(_, partitions.value))
      .groupByKey(numOfPartitions)
      .mapValues(_.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
        case (labeledMap, (partition, point)) =>
          labeledMap.get(point) match {
            case Some(found) => {
              if (found.flag == Flag.Noise && point.flag != Flag.Noise) {
                found.cluster = globalClusterIds.value((partition, point.cluster))
                labeledMap + (found -> found)
              } else if (point.flag == Flag.Noise) {
                labeledMap
              } else {
                val expected = globalClusterIds.value((partition, point.cluster))
                if (found.cluster != expected) {
                  logDebug(s"Dual assignment: ${found.cluster} and $expected for ${point}")
                  logDebug(s"${found.flag}, ${found.cluster}")
                  logDebug(s"${point.flag}, ${point.cluster}")
                }
                labeledMap
              }

            }
            case _ => if (point.flag != Flag.Noise) {
              point.cluster = globalClusterIds.value((partition, point.cluster))
              labeledMap + (point -> point)
            } else {
              labeledMap + (point -> point)
            }

          }

      }))
      .flatMapValues(_.values)
      .values

    val finalPartitions = localPartitionsWithMarginsAndIndex.map({
      case ((_, p, _), index) => (index, p)
    })
    

    new DBSCANModel(finalPartitions, labeledPoints)

  }

  /**
   * Returns all the aliases for a particular cluster by scanning the given (partition, points)
   *
   * The given iterable of (partition, points) may contain a set of duplicate points.
   * If a duplicate is found, the cluster name of the first instance and the cluster name
   * of subsequent points, are assumed to be aliases of one another.
   *
   *   @param partitionPointPairs the points to examine
   *   @return a set of cluster name pairs, where each entry in the pair refers to the same cluster
   */
  private def findClusterAliases(
    partitionPointPairs: Iterable[PartitionPointPair]): Set[(GlobalClusterId, GlobalClusterId)] = {

    val (_, aliases) =
      partitionPointPairs
        .foldLeft((
          Map[DBSCANPoint, GlobalClusterId](),
          Set[(GlobalClusterId, GlobalClusterId)]())) {

          case ((pointsToCluster, aliases), (id, point)) => {

            pointsToCluster.get(point) match {
              case Some(label) =>
                (pointsToCluster, aliases + ((label, (id, point.cluster))))
              case None =>
                (pointsToCluster + (point -> (id, point.cluster)), aliases)
            }

          }

        }

    aliases

  }

  private def isInsideMargin(point: DBSCANPoint, partitions: List[(MarginSet, Int)]): Boolean = {

    val containingPartitions = partitions
      .filter({
        case ((inner, main, outer), _) =>
          main.contains(point) && !inner.almostContains(point)
      })
      .size

    assert(containingPartitions <= 1)

    containingPartitions > 0
  }

  private def toPointWithMainPartition(
    partitionPoint: PartitionPointPair,
    partitions: List[(MarginSet, Int)]): (Int, (Int, DBSCANLabeledPoint)) = {
    partitionPoint match {
      case (partition, point) =>
        partitions
          .filter({ case ((inner, main, outer), _) => main.contains(point) })
          .head match {
            case (_, newPartition) => (newPartition, (partition, point))
          }
    }
  }

  private def toGridBox(point: DBSCANPoint): DBSCANBox = {
    val x = snapToGrid(point.x)
    val y = snapToGrid(point.y)
    DBSCANBox(x, y, x + gridSize, y + gridSize)
  }

  private def snapToGrid(p: Double): Double =
    (shiftIfNegative(p) / gridSize).intValue * gridSize

  private def shiftIfNegative(p: Double): Double = if (p < 0) p - gridSize else p

}
