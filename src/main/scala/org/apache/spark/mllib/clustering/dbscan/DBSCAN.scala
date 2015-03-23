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

object DBSCAN {

  def fit(eps: Float,
          minPoints: Int,
          data: RDD[DBSCANPoint],
          parallelism: Int): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, parallelism).fit(data)

  def fit(eps: Float, minPoints: Int, data: RDD[DBSCANPoint]): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, data.context.defaultParallelism).fit(data)

}

class DBSCAN(eps: Float, minPoints: Int, parallelism: Int) extends Serializable with Logging {

  val boxSize = 2 * eps

  def fit(vectors: RDD[DBSCANPoint]): RDD[LabeledVector] = {

    val localBoxesWithCount =
      vectors
        .map(v => (toBox(v), v))
        .aggregateByKey(0)(
          (acc, v) => acc + 1,
          (acc1, acc2) => acc1 + acc2)
        .collect()

    val dataSize = localBoxesWithCount.foldLeft(0)(
      (acc, boxWithCount) => boxWithCount match {
        case (box, count) => acc + count
      })

    val partitionSize = dataSize / parallelism
    val totalBoxes = localBoxesWithCount.length

    logDebug(s"Boxes: $totalBoxes")
    logDebug(s"Partitionsize $partitionSize")
    logDebug(s"Datasize $dataSize")

    val localPartitions = SpaceSplitter.findSplits(localBoxesWithCount, partitionSize, boxSize)
    localPartitions.foreach({
      case (b, c) => logInfo(SpaceSplitter.toRectangle(b, 0) + " # count:" + c)
    })

    val localPartitionsWithMargins = localPartitions.map({
      case (b, _) => toMargins(b)
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
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val duplicatedCount = duplicated.keys.count()
    logInfo(s"After duplication $duplicatedCount")

    val clustered = duplicated.flatMapValues(localDBSCAN)
      .persist(StorageLevel.DISK_ONLY)

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
            ((vector.point, partition), (partition, vector))
          } else {
            ((vector.point, findPartitionContaining(vector.point, partitions.value)),
              (partition, vector))
          }
      })
      .groupByKey(new PointIDPartitioner(localPartitionsWithMargins.length))
      .map({
        case ((point, partition), labeledPoints) =>
          labeledPoints.foldLeft(null: LabeledVector)(
            (acc, tuple) => tuple match {
              case (origPartition, vector) =>
                if (acc == null) {
                  if (vector.label != Noise) {
                    vector.label = globalClusterIds.value((origPartition, vector.label))
                  }
                  vector
                } else {

                  if (vector.label != Noise) {

                    if (acc.label == Noise) {
                      acc.label = globalClusterIds.value((origPartition, vector.label))
                    } else {
                      val expected = globalClusterIds.value((origPartition, vector.label))
                      if (acc.label != expected) {
                        val actual = acc.label
                        val point = acc.point
                        logDebug(s"Dual assignment: $actual and $expected for $point")
                      }

                    }

                  }

                  acc
                }
            })

      })
  }

  def findPartitionContaining(point: DBSCANPoint, partitions: List[(Margins, Int)]): Int =
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

  def contiguous(src: Box, others: List[(Margins, Int)]): List[Int] =
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

  def localDBSCAN(points: Iterable[LabeledVector]): TraversableOnce[LabeledVector] = {
    new LocalDBSCAN(eps, minPoints).fit(points)
  }

  def toMargins(box: Box): Margins =
    (Box(box.x + eps, box.y + eps, box.x2 - eps, box.y2 - eps),
      box,
      Box(box.x - eps, box.y - eps, box.x2 + eps, box.y2 + eps))

  def toBox(point: DBSCANPoint): Box = {
    val x = snapToGrid(point.x)
    val y = snapToGrid(point.y)
    Box(x, y, x + boxSize, y + boxSize)

  }

  def snapToGrid(p: Double): Float =
    (lower(p) / boxSize).intValue * boxSize

  def lower(p: Double): Double = if (p < 0) p - boxSize else p

}
