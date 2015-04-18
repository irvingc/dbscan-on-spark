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

import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.annotation.tailrec

object AdjacencyGraph {

  def apply[T](adjacencies: Seq[(T, T)]): AdjacencyGraph[T] = {
    adjacencies.foldLeft(new AdjacencyGraph[T]())({
      case(graph, (from, to)) => graph.insertAdjacency(from, to)
    })
  }

}

class AdjacencyGraph[T] {

  private val nodes = Map[T, Set[T]]()

  def vertexes() = nodes.keys

  def addVertex(v: T) {
    if (!nodes.contains(v)) {
      nodes += (v -> Set())
    }
  }

  def insertAdjacency(from: T, to: T) : AdjacencyGraph[T] = {
    insertOne(from, to)
    insertOne(to, from)
    this
  }

  def getAdjacent(from: T): List[T] = {
    getAdjacent(from, Set()).toList
  }

  private def getAdjacent(from: T, visited: Set[T]): Set[T] = {

    val matches = Set[T]()

    if (!visited.contains(from)) {
      visited += from

      if (nodes.contains(from)) {
        nodes(from).foreach(a => {
          matches += a
          matches ++= getAdjacent(a, visited)
        })
      }
    }

    matches

  }

  private def insertOne(one: T, other: T) {
    if (nodes.contains(one)) {
      nodes(one) += other
    } else {
      nodes += (one -> Set(other))
    }
  }

}
