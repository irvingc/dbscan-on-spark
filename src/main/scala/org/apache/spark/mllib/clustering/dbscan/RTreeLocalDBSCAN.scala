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
import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.Point
import com.github.davidmoten.rtree.geometry.Geometries
import com.github.davidmoten.rtree.Entry
import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer

class RTreeLocalDBSCAN(eps: Double, minPoints: Int) extends Logging {

  def fit(vectors: Iterable[LabeledVector]): Iterable[LabeledVector] = {

    logInfo(s"About to start fitting")

    val tree = vectors.foldLeft(RTree.star().create[LabeledVector, Point]())(
      (tempTree, vector) => tempTree.add(vector, Geometries.point(vector.point.x, vector.point.y)))

    var cluster = Unlabeled

    val entriesIterable = tree.entries().toBlocking().toIterable()

    val entries = entriesIterable.iterator()

    while (entries.hasNext()) {
      val entry = entries.next()
      val point = entry.geometry()
      val label = entry.value()

      if (!label.visited) {

        label.visited = true

        val neighbors = tree.search(point, eps)

        val count = neighbors.count().toBlocking().single()
        if (count < minPoints) {
          label.label = Noise
        } else {
          cluster += 1
          val it = neighbors.toBlocking().toIterable().iterator()
          expandCluster(label, it, tree, cluster)
        }

      }
    }

    val entriesLabeled = entriesIterable.iterator()

    val labeled = ListBuffer[LabeledVector]()

    while (entriesLabeled.hasNext()) {

      val entry = entriesLabeled.next()
      val point = entry.geometry()
      val label = entry.value()

      labeled += label

    }

    logInfo("done...")

    labeled
  }

  def expandCluster(
    label: LabeledVector,
    neighbors: java.util.Iterator[Entry[LabeledVector, Point]],
    tree: RTree[LabeledVector, Point],
    cluster: Int): Unit = {

    label.isCore = true
    label.label = cluster

    val left = new Queue[java.util.Iterator[Entry[LabeledVector, Point]]]

    left.enqueue(neighbors)

    while (left.nonEmpty) {

      val curNeighbors = left.dequeue()

      while (curNeighbors.hasNext()) {
        val neighbor = curNeighbors.next()

        val neighborLabel = neighbor.value()

        if (!neighborLabel.visited) {
          neighborLabel.visited = true
          neighborLabel.label = cluster

          val neighborNeighbors = tree.search(neighbor.geometry(), eps)

          if (neighborNeighbors.count().toBlocking().single() >= minPoints) {
            neighborLabel.isCore = true
            val it = neighborNeighbors.toBlocking().toIterable().iterator()
            left.enqueue(it)
          } else {
            neighborLabel.isBorder = true
          }

        } else if (neighborLabel.label == Noise) {
          neighborLabel.label = cluster
          neighborLabel.isBorder = true
        }

      }

    }

  }

}
