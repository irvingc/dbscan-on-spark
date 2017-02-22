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

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.dbscan.DBSCANLabeledPoint.Flag

import archery.Box
import archery.Entry
import archery.Point
import archery.RTree

/**
 * An implementation of DBSCAN using an R-Tree to improve its running time
 */
class LocalDBSCANArchery(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps * eps

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {

    val tree = points.foldLeft(RTree[DBSCANLabeledPoint]())(
      (tempTree, p) =>
        tempTree.insert(
          Entry(Point(p.x.toFloat, p.y.toFloat), new DBSCANLabeledPoint(p))))

    var cluster = DBSCANLabeledPoint.Unknown

    tree.entries.foreach(entry => {

      val point = entry.value

      if (!point.visited) {
        point.visited = true

        val neighbors = tree.search(toBoundingBox(point), inRange(point))

        if (neighbors.size < minPoints) {
          point.flag = Flag.Noise
        } else {
          cluster += 1
          expandCluster(point, neighbors, tree, cluster)
        }

      }

    })

    logDebug(s"total: $cluster")

    tree.entries.map(_.value).toIterable

  }

  private def expandCluster(
    point: DBSCANLabeledPoint,
    neighbors: Seq[Entry[DBSCANLabeledPoint]],
    tree: RTree[DBSCANLabeledPoint],
    cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    val left = Queue(neighbors)

    while (left.nonEmpty) {

      left.dequeue().foreach(neighborEntry => {

        val neighbor = neighborEntry.value

        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = tree.search(toBoundingBox(neighbor), inRange(neighbor))

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            left.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }
        }

        if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
          neighbor.cluster = cluster
          neighbor.flag = Flag.Border
        }

      })

    }

  }

  private def inRange(point: DBSCANPoint)(entry: Entry[DBSCANLabeledPoint]): Boolean = {
    entry.value.distanceSquared(point) <= minDistanceSquared
  }

  private def toBoundingBox(point: DBSCANPoint): Box = {
    Box(
      (point.x - eps).toFloat,
      (point.y - eps).toFloat,
      (point.x + eps).toFloat,
      (point.y + eps).toFloat)
  }

}
