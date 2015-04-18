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
package org.apache.spark.mllib.clustering.dbscan.regen

import scala.annotation.tailrec

object DBSCANGraph {

  def apply[T](): DBSCANGraph[T] = new DBSCANGraph(Map[T, Set[T]]())

  private def apply[T](nodes: Map[T, Set[T]]): DBSCANGraph[T] = {
    new DBSCANGraph(nodes)
  }

}

class DBSCANGraph[T] private (nodes: Map[T, Set[T]]) extends Serializable {

  def addVertex(v: T): DBSCANGraph[T] = {
    nodes.get(v) match {
      case None    => DBSCANGraph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }

  def insertEdge(from: T, to: T): DBSCANGraph[T] = {
    nodes.get(from) match {
      case None       => DBSCANGraph(nodes + (from -> Set(to)))
      case Some(edge) => DBSCANGraph(nodes + (from -> (edge + to)))
    }
  }

  def connect(from: T, to: T): DBSCANGraph[T] = {
    insertEdge(from, to).insertEdge(to, from)
  }

  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]())
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
