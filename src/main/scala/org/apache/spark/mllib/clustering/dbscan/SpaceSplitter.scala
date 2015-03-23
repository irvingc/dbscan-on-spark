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

object SpaceSplitter {

  def findSplits(boxesWithCount: Seq[BoxWithCount], limit: Long, boxSize: Float) =
    partition(boxesWithCount, limit, boxSize)

  def partition(allBoxes: Seq[BoxWithCount], limit: Long, boxSize: Float) =
    partitionr(List(findBoundingBox(allBoxes)), List[BoxWithCount](), allBoxes, limit, boxSize)

  def findBoundingBox(boxes: Seq[BoxWithCount]): Box = {

    val (x, y, x2, y2) = boxes
      .foldLeft((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))(
        (acc, box) => acc match {
          case (x, y, x2, y2) => {
            (x.min(box._1.x), y.min(box._1.y), x2.max(box._1.x2), y2.max(box._1.y2))
          }

        })

    Box(x, y, x2, y2)
  }

  def partitionr(
    topartition: List[Box],
    partitions: List[BoxWithCount],
    all: Seq[BoxWithCount],
    limit: Long,
    boxSize: Float): List[BoxWithCount] = {

    topartition match {
      case head :: rest => {
        if (pointsinBox(head, all) > limit) {

          costBasedBinarySplit(head, all, boxSize) match {
            case (s1, s2, c) =>
              if (s1 == head) {
                partitionr(rest, (s1, pointsinBox(s1, all)) :: partitions, all, limit, boxSize)
              } else {
                partitionr(s1 :: s2 :: rest, partitions, all, limit, boxSize)
              }
          }
        } else {
          partitionr(rest, (head, pointsinBox(head, all)) :: partitions, all, limit, boxSize)
        }

      }
      case Nil => partitions
    }
  }

  def toRectangle(b: Box, i: Int): String =
    s"Rectangle((${b.x},${b.y}),${b.x2 - b.x},${b.y2 - b.y},edgecolor=colors[$i],fc=colors[$i]),"

  def cost(space: Box, boundary: Box, boxes: Seq[BoxWithCount]): Int = {
    val spaceCount = pointsinBox(space, boxes)
    val boundaryCount = pointsinBox(boundary, boxes)
    ((boundaryCount / 2) - spaceCount).abs
  }

  def pointsinBox(space: Box, boxes: Seq[BoxWithCount]): Int = {
    boxes
      .filter({ case (b, _) => space.contains(b) })
      .foldLeft(0)(
        (acc, b) => b._2 + acc)
  }

  def costBasedBinarySplit(
      boundary: Box, 
      boxes: Seq[BoxWithCount], 
      boxSize: Float): (Box, Box, Int) =
    allBoxesWith(boundary, boxSize)
      .foldLeft((Box.empty, Box.empty, Int.MaxValue))({
        case ((s1, s2, minCost), box) =>
          (
            (cost: Int) =>
              if (cost < minCost) {
                (box, complement(box, boundary), cost)
              } else {
                (s1, s2, minCost)
              })(cost(box, boundary, boxes))
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
