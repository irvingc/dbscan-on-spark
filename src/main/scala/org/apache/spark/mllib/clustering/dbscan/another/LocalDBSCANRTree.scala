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

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.clustering.dbscan.another.DBSCANLabeledPoint.Flag
import com.github.davidmoten.rtree.Entry
import com.github.davidmoten.rtree.RTree
import com.github.davidmoten.rtree.geometry.Geometries
import com.github.davidmoten.rtree.geometry.Point
import java.util.Iterator

class LocalDBSCANRTree(eps: Double, minPoints: Int) extends Logging {

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {

    logInfo(s"About to start fitting")

    val tree = points.foldLeft(RTree.star().create[DBSCANLabeledPoint, Point]())(
      (tempTree, point) =>
        tempTree.add(new DBSCANLabeledPoint(point), Geometries.point(point.x, point.y)))

    var cluster = DBSCANLabeledPoint.Unknown

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
          label.flag = Flag.Noise
        } else {
          cluster += 1
          val it = neighbors.toBlocking().toIterable().iterator()
          expandCluster(label, it, tree, cluster)

        }

      }
    }

    val entriesLabeled = entriesIterable.iterator()

    val labeled = ListBuffer[DBSCANLabeledPoint]()

    while (entriesLabeled.hasNext()) {

      val entry = entriesLabeled.next()
      val point = entry.geometry()
      val label = entry.value()

      labeled += label

    }

    logDebug(s"total: $cluster")

    labeled
  }

  def expandCluster(
    label: DBSCANLabeledPoint,
    neighbors: Iterator[Entry[DBSCANLabeledPoint, Point]],
    tree: RTree[DBSCANLabeledPoint, Point],
    cluster: Int): Unit = {

    label.flag = Flag.Core
    label.cluster = cluster

    val allNeighbors = Queue(neighbors)

    while (allNeighbors.nonEmpty) {

      val curNeighbors = allNeighbors.dequeue()

      while (curNeighbors.hasNext()) {
        val neighbor = curNeighbors.next()

        val neighborLabel = neighbor.value()

        if (!neighborLabel.visited) {
          neighborLabel.visited = true
          neighborLabel.cluster = cluster

          val neighborNeighbors = tree.search(neighbor.geometry(), eps)

          if (neighborNeighbors.count().toBlocking().single() >= minPoints) {
            neighborLabel.flag = Flag.Core
            val it = neighborNeighbors.toBlocking().toIterable().iterator()
            allNeighbors.equals(it)
          } else {
            neighborLabel.flag = Flag.Border
          }

        } else if (neighborLabel.flag == Flag.Noise) {
          neighborLabel.cluster = cluster
          neighborLabel.flag = Flag.Border
        }

      }

    }

  }

}
