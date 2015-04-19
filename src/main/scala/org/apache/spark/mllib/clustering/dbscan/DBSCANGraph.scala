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

import scala.annotation.tailrec

/**
 * Top level method for creating a DBSCANGraph
 */
object DBSCANGraph {

  /**
   * Create an empty graph
   */
  def apply[T](): DBSCANGraph[T] = new DBSCANGraph(Map[T, Set[T]]())

}

/**
 * An immutable unweighted graph with vertexes and edges
 */
class DBSCANGraph[T] private (nodes: Map[T, Set[T]]) extends Serializable {

  /**
   * Add the given vertex `v` to the graph
   *
   */
  def addVertex(v: T): DBSCANGraph[T] = {
    nodes.get(v) match {
      case None    => new DBSCANGraph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }

  /**
   * Insert an edge from `from` to `to`
   */
  def insertEdge(from: T, to: T): DBSCANGraph[T] = {
    nodes.get(from) match {
      case None       => new DBSCANGraph(nodes + (from -> Set(to)))
      case Some(edge) => new DBSCANGraph(nodes + (from -> (edge + to)))
    }
  }

  /**
   * Insert a vertex from `one` to `another`, and from `another` to `one`
   *
   */
  def connect(one: T, another: T): DBSCANGraph[T] = {
    insertEdge(one, another).insertEdge(another, one)
  }

  /**
   * Find all vertexes that are reachable from `from`
   */
  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]()) - from
  }

  @tailrec
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] = {

    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) =>
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }

  }

}
