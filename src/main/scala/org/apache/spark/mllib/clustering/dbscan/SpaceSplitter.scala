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

import archery.Box
import archery.RTree
import archery.Entry

object SpaceSplitter {

  def findSplits(boxesWithCount: Seq[BoxWithCount], limit: Long, boxSize: Float) =
    partition(toRTree(boxesWithCount), limit, boxSize)

  def partition(tree: RTree[Int], limit: Long, boxSize: Float) =
    partitionr(List(tree.root.box), List[Box](), tree, limit, boxSize)

  def partitionr(
    topartition: List[Box],
    partitions: List[Box],
    tree: RTree[Int],
    limit: Long,
    boxSize: Float): List[Box] = {

    topartition match {
      case head :: rest => {
        if (pointsinBox(head, tree) > limit) {
          costBasedBinarySplit(head, tree, boxSize) match {
            case (s1, s2, _) =>
              partitionr(s1 :: s2 :: rest, partitions, tree, limit, boxSize)
          }
        } else {
          partitionr(rest, head :: partitions, tree, limit, boxSize)
        }

      }
      case Nil => partitions
    }
  }

  def toRectangle(b: Box, i: Int): String =
    s"Rectangle((${b.x},${b.y}), ${b.x2 - b.x}, ${b.y2 - b.y}, edgecolor=colors[$i], fc=colors[$i]"

  def toRTree(boxesWithCount: Seq[BoxWithCount]): RTree[Int] =
    RTree(boxesWithCount.map(toEntry): _*)

  def toEntry(boxWithCount: BoxWithCount): Entry[Int] = boxWithCount match {
    case (box, count) => Entry(box, count)
  }

  def cost(space: Box, boundary: Box, tree: RTree[Int]): Long = {
    val spaceCount = pointsinBox(space, tree)

    val boundaryCount = pointsinBox(boundary, tree)

    ((boundaryCount / 2) - spaceCount).abs
  }

  def pointsinBox(space: Box, tree: RTree[Int]): Long =
    tree.foldSearch(space, 0)({
      case (accumulator, entry) => accumulator + entry.value
    })

  def costBasedBinarySplit(boundary: Box, tree: RTree[Int], boxSize: Float): (Box, Box, Long) =

    allBoxesWith(boundary, boxSize)
      .foldLeft((Box.empty, Box.empty, Long.MaxValue))({
        case ((s1, s2, minCost), box) =>
          (
            (cost: Long) =>
              if (cost < minCost) {
                (box, complement(box, boundary), cost)
              } else {
                (s1, s2, minCost)
              })(cost(box, boundary, tree))
      })

  def complement(box: Box, boundary: Box): Box =
    if (box.x == boundary.x && box.y == boundary.y) {
      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) {
        if (box.y2 == boundary.y2) {
          Box(box.x2, box.y, boundary.x2, boundary.y2)
        } else if (box.x2 == boundary.x2) {
          Box(box.x, box.y2, boundary.x2, boundary.y2)
        } else {
          throw new IllegalArgumentException("Box is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("box is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal boxes")
    }

  def allBoxesWith(boundary: Box, boxSize: Float): Set[Box] =
    (
      (boundary.x + boxSize)
      .to(boundary.x2).by(boxSize)
      .map(x => Box(boundary.x, boundary.y, x, boundary.y2)) ++
      (boundary.y + boxSize)
      .to(boundary.y2).by(boxSize)
      .map(y => Box(boundary.x, boundary.y, boundary.x2, y)))
      .toSet

}
