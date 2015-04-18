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
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.Logging
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import org.apache.spark.util.collection.OpenHashMap
import scala.collection.immutable.HashSet

object DBSCAN {

  def fit(eps: Float,
          minPoints: Int,
          data: RDD[DBSCANPoint],
          parallelism: Int): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, parallelism).fit(data)

  def fit(eps: Float, minPoints: Int, data: RDD[DBSCANPoint]): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, data.context.defaultParallelism).fit(data)

  private[DBSCAN] def toMargins(box: DBSCANBox, eps: Double): Margins =
    (DBSCANBox(box.x + eps, box.y + eps, box.x2 - eps, box.y2 - eps),
      box,
      DBSCANBox(box.x - eps, box.y - eps, box.x2 + eps, box.y2 + eps))

  private[DBSCAN] def findPartitionContaining(
    point: DBSCANPoint,
    partitions: List[(Margins, Int)]): Int =
    partitions
      .filter({
        case (margins, _) => margins match {
          case (inner, main, outer) => main.contains(point)
        }
      })
      .map({
        case (_, id) => id
      })
      .head

  private[DBSCAN] def toDBSCANBox(point: DBSCANPoint, boxSize: Double): DBSCANBox = {
    val x = snapToGrid(point.x, boxSize)
    val y = snapToGrid(point.y, boxSize)
    DBSCANBox(x, y, x + boxSize, y + boxSize)

  }

  private[DBSCAN] def snapToGrid(p: Double, boxSize: Double): Double =
    (lower(p, boxSize) / boxSize).intValue * boxSize

  private[DBSCAN] def lower(p: Double, boxSize: Double): Double = if (p < 0) p - boxSize else p

}

class DBSCAN(eps: Double, minPoints: Int, parallelism: Int) extends Serializable with Logging {

  val boxSize = 2 * eps

  def fit(vectors: RDD[DBSCANPoint]): RDD[LabeledVector] = {

    val localDBSCANBoxesWithCount =
      vectors
        .map(v => (DBSCAN.toDBSCANBox(v, boxSize), v))
        .aggregateByKey(0)(
          (acc, v) => acc + 1,
          (acc1, acc2) => acc1 + acc2)
        .collect()

    val dataSize = localDBSCANBoxesWithCount.foldLeft(0)(
      (acc, boxWithCount) => boxWithCount match {
        case (box, count) => acc + count
      })

    val partitionSize = dataSize / parallelism
    val totalDBSCANBoxes = localDBSCANBoxesWithCount.length

    logDebug(s"DBSCANBoxes: $totalDBSCANBoxes")
    logDebug(s"Partitionsize $partitionSize")
    logDebug(s"Datasize $dataSize")

    val localPartitions =
      SpaceSplitter.findSplits(localDBSCANBoxesWithCount, partitionSize, boxSize)

    localPartitions.foreach({
      case (b, c) => logInfo(SpaceSplitter.toRectangle(b, 0) + " # count:" + c)
    })

    val localPartitionsWithMargins = localPartitions.map({
      case (b, _) => DBSCAN.toMargins(b, eps)
    }).zipWithIndex

    val foundLocalPartitions = localPartitionsWithMargins.length
    logDebug(s"Found: $foundLocalPartitions")

    val localAdjancencyMap = findAdjacencies(localPartitionsWithMargins)

    printPartitions(localPartitionsWithMargins)
    printAdjancencyMap(localAdjancencyMap)

    val partitions = vectors.context.broadcast(localPartitionsWithMargins)
    val adjacencies = vectors.context.broadcast(localAdjancencyMap)

    val duplicated = vectors
      .flatMap(p => {
        partitions.value
          .map({
            case (margins, id) => (id, MarginClassifier.classify(p, margins))
          })
          .filter({
            case (id, classification) => classification != MarginClassifier.NotBelonging
          })
          .map({
            case (id, classification) => (id, new LabeledVector(p, classification))
          })
      })
      .groupByKey(localPartitionsWithMargins.length)

    val clustered =
      duplicated
        .flatMapValues(points => new RTreeLocalDBSCAN(eps, minPoints).fit(points))
        .cache

    val clusteredCount = clustered.count()
    logInfo(s"After clustering: $clusteredCount")

    val ap = clustered
      .filter({
        case (id, vector) =>
          vector.isCore && vector.classification == MarginClassifier.Inner &&
            adjacencies.value.contains(id)
      })
      .flatMap({
        case (id, vector) => adjacencies.value(id).map(other => ((id, other), vector))
      })

    val bp = clustered
      .filter({
        case (id, vector) =>
          (vector.isCore || vector.isBorder) &&
            vector.classification == MarginClassifier.Outer &&
            adjacencies.value.contains(id)
      })
      .flatMap({
        case (id, vector) => adjacencies.value(id).map(other => ((other, id), vector))
      })

    val localIntersections = ap.cogroup(bp)
      .flatMapValues({
        case (aps, bps) => aps.flatMap(aentry => {
          bps
            .filter(bentry => bentry.point == aentry.point)
            .map(bentry => (aentry.label, bentry.label))
        })
      })
      .distinct()
      .map({
        case ((apartition, bpartition), (alabel, blabel)) =>
          ((apartition, alabel), (bpartition, blabel))
      })
      .collect()

    log.debug("Intersections")
    localIntersections.foreach(l => logDebug(l.toString()))

    val localListOfClusters = clustered
      .filter({
        case (_, vector) => vector.label != Noise
      })
      .mapValues(_.label)
      .distinct()
      .collect()

    val (_, localGlobalClusterIds) = localListOfClusters.foldLeft((1, Map[(Int, Int), Int]()))(
      (acc, clusterId) => acc match {
        case (id, map) => {
          if (!map.contains(clusterId)) {
            (
              id + 1,
              map + (clusterId -> id) ++
              localIntersections
              .filter({
                case (a, b) => a == clusterId
              })
              .map({
                case (a, b) => (b, id)
              })
              .toMap)

          } else {
            (id, map)
          }
        }
      })

    logDebug("Global Cluster IDs")
    localGlobalClusterIds.foreach(id => logDebug(id.toString()))
    logInfo(s"Total Clusters: ${localGlobalClusterIds.size}")

    val globalClusterIds = vectors.context.broadcast(localGlobalClusterIds)

    clustered
      .map({
        case (partition, vector) =>
          if (MarginClassifier.isInterior(vector.classification)) {
            (partition, (partition, vector))
          } else {
            (DBSCAN.findPartitionContaining(vector.point, partitions.value),
              (partition, vector))
          }
      })
      .groupByKey(localPartitionsWithMargins.length)
      .mapValues(_.foldLeft(Map[DBSCANPoint, LabeledVector]())(
        (labeled, vectorTuple) => vectorTuple match {
          case (partition, vector) =>

            labeled.get(vector.point) match {
              case Some(found) => {
                if (found.label == Noise && vector.label != Noise) {
                  found.label = globalClusterIds.value((partition, vector.label))
                  labeled + (found.point -> found)
                } else if (vector.label == Noise) {
                  labeled
                } else {
                  val expected = globalClusterIds.value((partition, vector.label))
                  if (found.label != expected) {
                    logDebug(s"Dual assignment: ${found.label} and $expected for ${vector.point}")
                    logDebug(s"${found.isBorder}, ${found.isCore}, ${found.label}")
                    logDebug(s"${vector.isBorder}, ${vector.isCore}, ${vector.label}")
                  }
                  labeled
                }

              }
              case _ => if (vector.label != Noise) {
                vector.label = globalClusterIds.value((partition, vector.label))
                labeled + (vector.point -> vector)
              } else {
                labeled + (vector.point -> vector)
              }

            }

        }))
      .flatMapValues(_.values)
      .values

  }

  def printAdjancencyMap(adjacencies: Map[Int, List[Int]]): Unit = {
    log.debug("Adjacencies:")
    adjacencies.foreach(a => log.debug(a.toString()))
  }

  def findAdjacencies(containers: List[(Margins, Int)]): Map[Int, List[Int]] =
    containers
      .map({
        case ((inner, main, outer), id) => (id, contiguous(main, containers))
      })
      .toMap

  def contiguous(src: DBSCANBox, others: List[(Margins, Int)]): List[Int] =
    others
      .filter({
        case ((inner, main, outer), id) => src != main && main.intersects(src)
      })
      .map({
        case (_, id) => id
      })

  def printPartitions(partitions: List[(Margins, Int)]): Unit = {
    log.debug("Partitions: ")
    partitions.foreach({
      case ((inner, main, outer), id) => {
        log.debug("{}", SpaceSplitter.toRectangle(main, id))
      }
    })
  }

}
