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

object DBSCANBox {

  val Precision = 0.0001

  /**
   * Creates a box with the given point as left corner and the given width and height
   */
  def apply(origin: DBSCANPoint, width: Double, height: Double) {
    DBSCANBox(origin.x, origin.y, origin.x + width, origin.y + height)
  }
}
/**
 * A box whose left corner is (x,y) and righ upper corner is (x2, y2)
 */
case class DBSCANBox(x: Double, y: Double, x2: Double, y2: Double) {

  /**
   * Returns whether other is contained by this box
   */
  def contains(other: DBSCANBox): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  /**
   * Returns whether point is contained by this box
   */
  def contains(point: DBSCANPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  def almostContains(point: DBSCANPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

  /**
   * Shrinks the box by the given amount
   */
  def contract(amount: Double): DBSCANBox = {
    DBSCANBox(x + amount, y + amount, x2 - amount, y2 - amount)
  }

  private def nearlyEqual(a: Double, b: Double, epsilon: Double): Boolean = {

    val diff = (a - b).abs

    if (a == b) {
      true
    } else if (a == 0 || b == 0 || diff < Double.MinValue) {
      diff < (epsilon * Float.MinValue)
    } else {
      diff / (a.abs + b.abs) < epsilon

    }

  }

}
