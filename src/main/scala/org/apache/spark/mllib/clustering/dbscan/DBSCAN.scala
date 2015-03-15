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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import archery.Box
import archery.Point

object DBSCAN {

  def toProjection(vector: Vector): ProjectedVector =
    (Point(vector(0).floatValue(), vector(1).floatValue()), vector)

  def fit(eps: Float, minPoints: Int, data: RDD[Vector], parallelism: Int): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, parallelism).fit(data)

  def fit(eps: Float, minPoints: Int, data: RDD[Vector]): RDD[LabeledVector] =
    new DBSCAN(eps, minPoints, data.context.defaultParallelism).fit(data)

}

class DBSCAN(eps: Float, minPoints: Int, parallelism: Int) extends Serializable with Logging {

  val boxSize = 2 * eps

  def fit(vectors: RDD[Vector]): RDD[LabeledVector] = {

    vectors.context.executorEnvs

    val projections = vectors.map(DBSCAN.toProjection).cache

    val localBoxesWithCount =
      projections
        .map(p => (toBox(p), p))
        .aggregateByKey(0)(
          (acc, projection) => acc + 1,
          (acc1, acc2) => acc1 + acc2)
        .collect()

    val dataSize = projections.count()
    val partitionSize = dataSize / parallelism

    val localPartitionsWithMargins = SpaceSplitter
      .findSplits(localBoxesWithCount, partitionSize, boxSize)
      .map(toMargins)
      .zipWithIndex

    val localAdjancencyMap = findAdjacencies(localPartitionsWithMargins)

    printPartitions(localPartitionsWithMargins)
    printAdjancencyMap(localAdjancencyMap)

    val partitions = vectors.context.broadcast(localPartitionsWithMargins)
    val adjacencies = vectors.context.broadcast(localAdjancencyMap)

    val duplicatedAndClassified = projections.flatMap(p => {
      partitions.value
        .map({
          case (margins, id) => (id, MarginClassifier.classify(p, margins))
        })
        .filter({
          case (id, classification) => classification != MarginClassifier.NotBelonging
        })
        .map({
          case (id, classification) => (id, new LabeledVector(p, id, classification))
        })
    })

    val clustered = duplicatedAndClassified
      .groupByKey(localPartitionsWithMargins.length)
      .mapPartitions(localDBSCAN, true)
      .flatMapValues(p => p)
      .cache

    val ap = clustered.filter({
      case (id, vector) => vector.isCore && vector.classification == MarginClassifier.Inner
    })

    val bp = clustered.filter({
      case (id, vector) =>
        (vector.isCore || vector.isBorder) &&
          vector.classification == MarginClassifier.Outer
    })

    val apWithId = ap.flatMap({
      case (id, vector) => adjacencies.value.getOrElse(id, List()).map(otherId => {
        ((id, otherId), vector)
      })
    })

    val bpWithId = bp.flatMap({
      case (id, vector) => adjacencies.value.getOrElse(id, List()).map(otherId => {
        ((otherId, id), vector)
      })
    })

    val localIntersections = apWithId.cogroup(bpWithId)
      .flatMapValues({
        case (aps, bps) => aps.flatMap(aentry => {
          bps
            .filter(bentry => bentry.point == aentry.point)
            .map(bentry =>
              ((bentry.partition, bentry.localLabel),
                (aentry.partition, aentry.localLabel)))
        })
      })
      .values
      .distinct()
      .collect()

    log.debug("Intersections")
    localIntersections.foreach(l => logDebug(l.toString()))

    val localListOfClusters = clustered
      .filter({
        case (_, vector) => vector.localLabel != Noise
      })
      .mapValues(_.localLabel)
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

    val globalClusterIds = vectors.context.broadcast(localGlobalClusterIds)

    clustered
      .map({
        case (partition, vector) =>
          if (MarginClassifier.isInterior(vector.classification)) {
            ((vector.point, partition), vector)
          } else {
            ((vector.point, findPartitionContaining(vector.point, partitions.value)), vector)
          }
      })
      .groupByKey(new PointIDPartitioner(localPartitionsWithMargins.length))
      .mapValues(vectors =>
        vectors.foldLeft(null: LabeledVector)(
          (labeled, vector) => if (labeled == null) {
            if (vector.localLabel != Noise) {
              vector.label = globalClusterIds.value((vector.partition, vector.localLabel))
            } else {
              vector.label = Noise
            }
            vector
          } else {

            if (labeled.label == Noise && vector.localLabel != Noise) {
              labeled.label = globalClusterIds.value((vector.partition, vector.localLabel))
            }

            if (labeled.label != Noise && vector.localLabel != Noise) {

              if (globalClusterIds.value.contains((vector.partition, vector.localLabel))) {
                val expected = globalClusterIds.value((vector.partition, vector.localLabel))
                if (labeled.label != expected) {
                  val actual = labeled.label
                  val point = labeled.point
                  logDebug(s"Dual assignment: $actual and $expected for $point")
                }
              }

            }

            labeled
          }))
      .values

  }

  def findPartitionContaining(point: Point, partitions: List[(Margins, Int)]): Int =
    partitions.filter({
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

  def localDBSCAN(partition: Partition): Partition = partition.map({
    case (partitionId, vectors) => {
      (partitionId,
        new LocalDBSCAN(eps, minPoints)
        .fit(vectors.toList))
    }
  })

  def toMargins(box: Box): Margins =
    (Box(box.x + eps, box.y + eps, box.x2 - eps, box.y2 - eps),
      box,
      Box(box.x - eps, box.y - eps, box.x2 + eps, box.y2 + eps))

  def toBox(projection: ProjectedVector): Box =
    projection match {
      case (point, _) => toBox(toCorner(point))

    }

  def toBox(corner: Point): Box =
    Box(corner.x, corner.y, corner.x + boxSize, corner.y + boxSize)

  def toCorner(point: Point): Point =
    Point(snapToGrid(point.x), snapToGrid(point.y))

  def snapToGrid(p: Float): Float =
    (lower(p) / boxSize).intValue * boxSize

  def lower(p: Float): Float = if (p < 0) p - boxSize else p

}
