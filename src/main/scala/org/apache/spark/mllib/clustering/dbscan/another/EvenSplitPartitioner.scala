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

import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi

/**
 * Top-level means for calling EvenSplitPartitioner
 */
object EvenSplitPartitioner {

  /**
   * Returns partitions that evenly split the space contained by the given boxes.
   *
   * Given a set of boxes and a count of the elements they contain, create a
   * space bounding all the boxes, and then split the space into boxes
   * that contain roughly the same number of elements.
   *
   * The splitting will stop once a box is below the maxPointsInPartition,
   * or it is smaller than the minimum size of the box, given by gridSize.
   *
   * @param boxesToSplitWithCount a set of boxes with a count of the points they contain
   * @param maxPointsInPartition the maximum number of points that can be contained in
   * each of the returned partitions
   * @param minimumLength the minimum length for a partition, that is, no partitions smaller
   * than minimumLength will be return
   * 
   */
  def findPartitions(
    boxesToSplitWithCount: Set[(DBSCANBox, Int)],
    maxPointsInPartition: Long,
    minimumLength: Double): List[(DBSCANBox, Int)] = {

    val partitioner = new EvenSplitPartitioner(
      boxesToSplitWithCount,
      maxPointsInPartition,
      minimumLength)

    val costFun = evenlySplitPoints(
      _: DBSCANBox,
      _: DBSCANBox,
      partitioner.pointsInBox(_: DBSCANBox))

    val partitions = partitioner.findPartitions(costFun)
    
    // remove empty partitions
    partitions.filter({ case (b, c) => c > 0})

  }

  /**
   * Returns the cost of the split in relation to the given space
   * 
   * A cost of 0 represents that the split divides the space in half.
   * Higher costs represent a less balanced split.
   * 
   * @param split the box for which to find the cost
   * @param space the space containing all the points
   * @param countPoints a function that returns the number of points in the given box
   */
  private def evenlySplitPoints(
    split: DBSCANBox,
    space: DBSCANBox,
    countPoints: (DBSCANBox) => Int): Int = {
    ((countPoints(space) / 2) - countPoints(split)).abs
  }
}
/**
 * Splits a number of boxes 
 */
private class EvenSplitPartitioner(
  boxesToSplitWithCount: Set[(DBSCANBox, Int)],
  maxPointsPerPartition: Long,
  minimumLength: Double) extends Logging {

  /**
   * Returns partitions that minimize the cost according to costFun
   * 
   * @param costFun a function that is utilized to find the cost of a given box.
   * The function takes two parameters, the first is a candidate split, and the 
   * second one is the original space.
   */
  def findPartitions(costFun: (DBSCANBox, DBSCANBox) => Int): List[(DBSCANBox, Int)] = {

    val boxes = boxesToSplitWithCount.view.map({ case (box, count) => box })
    val bounding = findBoundingDBSCANBox(boxes)
    val toPartition = List((bounding, pointsInBox(bounding)))
    val partitioned = List[(DBSCANBox, Int)]()

    logDebug("About to start partitioning")
    val partitions = partition(toPartition, partitioned, costFun)
    logDebug("Done")

    partitions
  }

  /**
   * Returns the partitions that minimize the costFun
   * 
   * @param remaining a list boxes that are yet to be partitioned
   * @param partitioned a list of boxes that have been partitioned
   * @param costFun the function used to determine the cost of each partition
   * 
   */
  @tailrec
  private def partition(
    remaining: List[(DBSCANBox, Int)],
    partitioned: List[(DBSCANBox, Int)],
    costFun: (DBSCANBox, DBSCANBox) => Int): List[(DBSCANBox, Int)] = {

    remaining match {
      case (box, count) :: rest =>
        if (count > maxPointsPerPartition) {

          if (canBeSplit(box)) {
            logTrace(s"About to split box: $box")
            val (split1, split2) = split(box, costFun)
            logTrace(s"Found split: $split1, $split2")
            val s1 = (split1, pointsInBox(split1))
            val s2 = (split2, pointsInBox(split2))
            partition(s1 :: s2 :: rest, partitioned, costFun)
          } else {
            logWarning(s"Can't split box: $box (count: $count) (maxSize: $maxPointsPerPartition)")
            partition(rest, (box, count) :: partitioned, costFun)
          }
        } else {
          partition(rest, (box, count) :: partitioned, costFun)
        }
      case Nil => partitioned
    }
  }

  /**
   * Returns the points contained by the given box
   */
  private def pointsInBox(box: DBSCANBox): Int = {
    boxesToSplitWithCount
      .view
      .filter({ case (grid, _) => box.contains(grid) })
      .foldLeft(0)({ case (count, (box, curCount)) => count + curCount })
  }

  /**
   * Returns a box that covers all the given boxes
   */
  private def findBoundingDBSCANBox(boxes: Iterable[DBSCANBox]): DBSCANBox = {

    val (x, y, x2, y2) = boxes
      .foldLeft((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))({
        case ((x, y, x2, y2), current) =>
          (x.min(current.x), y.min(current.y), x2.max(current.x2), y2.max(current.y2))
      })

    DBSCANBox(x, y, x2, y2)
  }

  /**
   * Returns true if the given box can be split into at least two boxes of minimumLength
   */
  private def canBeSplit(box: DBSCANBox): Boolean = {
    (box.x2 - box.x > minimumLength * 2 || box.y2 - box.y > minimumLength * 2)
  }

  /**
   * Returns the two boxes that split the given box while minimizing costFun
   * 
   * @param box the box to split
   * @param costFun the function using to determine the cost of the split
   */
  private def split(
    box: DBSCANBox,
    costFun: (DBSCANBox, DBSCANBox) => Int): (DBSCANBox, DBSCANBox) = {

    val possibleSplits = findPossibleSplits(box)

    val smallestSplit: DBSCANBox = possibleSplits
      .reduceLeft((smallest, candidate) =>
        if (costFun(candidate, box) < costFun(smallest, box)) {
          candidate
        } else {
          smallest
        })

    (smallestSplit, complement(smallestSplit, box))

  }

  /**
   * Returns all the possible ways the given box can be split
   */
  private def findPossibleSplits(box: DBSCANBox): Set[DBSCANBox] = {

    val xSplits = (box.x + minimumLength) to box.x2 by minimumLength

    val ySplits = (box.y + minimumLength) to box.y2 by minimumLength

    val splits =
      xSplits.map(x => DBSCANBox(box.x, box.y, x, box.y2)) ++
        ySplits.map(y => DBSCANBox(box.x, box.y, box.x2, y))

    splits.toSet
  }

  /**
   * Returns the box that covers the space inside boundary that is not covered by box
   */
  private def complement(box: DBSCANBox, boundary: DBSCANBox): DBSCANBox =
    if (box.x == boundary.x && box.y == boundary.y) {
      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) {
        if (box.y2 == boundary.y2) {
          DBSCANBox(box.x2, box.y, boundary.x2, boundary.y2)
        } else if (box.x2 == boundary.x2) {
          DBSCANBox(box.x, box.y2, boundary.x2, boundary.y2)
        } else {
          throw new IllegalArgumentException("DBSCANBox is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("box is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal boxes")
    }

}
