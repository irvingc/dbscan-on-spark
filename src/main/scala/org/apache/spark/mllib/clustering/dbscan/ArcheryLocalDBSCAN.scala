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
import archery.RTree
import archery.Entry
import archery.Point
import archery.Box
import scala.collection.mutable.Queue

class ArcheryLocalDBSCAN(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps * eps
  val halfEps = eps / 2

  def fit(vectors: Iterable[LabeledVector]): Iterable[LabeledVector] = {

    val tree = vectors.foldLeft(RTree[LabeledVector]())(
      (tempTree, v) => tempTree.insert(Entry(Point(v.point.x.toFloat, v.point.y.toFloat), v)))

    var cluster = Unlabeled

    for (entry <- tree.entries) {
      val vector = entry.value

      if (!vector.visited) {
        vector.visited = true

        val neighbors = tree.search(toBoundingBox(vector.point))

        if (neighbors.size < minPoints) {
          vector.label = Noise
        } else {
          cluster += 1
          expandCluster(vector, neighbors, tree, cluster)
        }

      }

    }

    logDebug(s"total: $cluster")

    tree.entries.map(_.value).toIterable

  }

  def zup(point: DBSCANPoint, entry: Entry[LabeledVector]): Boolean = {
    entry.geom.distanceSquared(Point(point.x.toFloat, point.y.toFloat)) <= minDistanceSquared
  }

  def expandCluster(
    vector: LabeledVector,
    neighbors: Seq[Entry[LabeledVector]],
    tree: RTree[LabeledVector],
    cluster: Int): Unit = {

    vector.isCore = true
    vector.label = cluster

    val left = new Queue[Seq[Entry[LabeledVector]]]()
    left.enqueue(neighbors)

    while (left.nonEmpty) {
      val curNeighbors = left.dequeue()

      for (neighborEntry <- curNeighbors) {

        val neighbor = neighborEntry.value

        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.label = cluster

          val neighborNeighbors = tree.search(toBoundingBox(neighbor.point))

          if (neighborNeighbors.size >= minPoints) {
            neighbor.isCore = true
            left.enqueue(neighborNeighbors)
          } else {
            neighbor.isBorder = true
          }
        } else if (neighbor.label == Noise) {
          neighbor.label = cluster
          neighbor.isBorder = true
        }

      }

    }

  }

  def toBoundingBox(point: DBSCANPoint): Box = {
    Box((point.x - eps).toFloat,
      (point.y - eps).toFloat,
      (point.x + eps).toFloat,
      (point.y + eps).toFloat)
  }

}
