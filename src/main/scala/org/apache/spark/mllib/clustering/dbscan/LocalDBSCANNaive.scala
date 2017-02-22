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
import org.apache.spark.mllib.linalg.Vectors

/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class LocalDBSCANNaive(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps * eps

  def samplePoint = Array(new DBSCANLabeledPoint(Vectors.dense(Array(0D, 0D))))

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {

    logInfo(s"About to start fitting")

    val labeledPoints = points.map { new DBSCANLabeledPoint(_) }.toArray

    val totalClusters =
      labeledPoints
        .foldLeft(DBSCANLabeledPoint.Unknown)(
          (cluster, point) => {
            if (!point.visited) {
              point.visited = true

              val neighbors = findNeighbors(point, labeledPoints)

              if (neighbors.size < minPoints) {
                point.flag = Flag.Noise
                cluster
              } else {
                expandCluster(point, neighbors, labeledPoints, cluster + 1)
                cluster + 1
              }
            } else {
              cluster
            }
          })

    logInfo(s"found: $totalClusters clusters")

    labeledPoints

  }

  private def findNeighbors(
    point: DBSCANPoint,
    all: Array[DBSCANLabeledPoint]): Iterable[DBSCANLabeledPoint] =
    all.view.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared
    })

  def expandCluster(
    point: DBSCANLabeledPoint,
    neighbors: Iterable[DBSCANLabeledPoint],
    all: Array[DBSCANLabeledPoint],
    cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    var allNeighbors = Queue(neighbors)

    while (allNeighbors.nonEmpty) {

      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = findNeighbors(neighbor, all)

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }

          if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }

      })

    }

  }

}
