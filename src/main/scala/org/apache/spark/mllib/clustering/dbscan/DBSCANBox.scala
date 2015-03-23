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

object Box {

  val Precision = 0.0001

  /**
   * This is an "inside-out" box that we use as a good starting
   * value. The nice thing about this, unlike Box(0,0,0,0), is that
   * when merging with another box we don't include an artifical
   * "empty" point.
   */
  val empty: Box = {
    val s = Math.sqrt(Double.MaxValue).toFloat
    val t = s + -2.0F * s
    Box(s, s, t, t)
  }
}

case class Box(x: Double, y: Double, x2: Double, y2: Double) {
  
  def contains(other: Box): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }
  
  def contains(point: DBSCANPoint) : Boolean = {
   x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2 
  }
  
  def intersects(other: Box): Boolean = {
    (x < other.x2 || nearlyEqual(x, other.x2, Box.Precision)) &&
      (other.x < x2 || nearlyEqual(x2, other.x, Box.Precision)) &&
      (y < other.y2 || nearlyEqual(y, other.y2, Box.Precision)) &&
      (other.y < y2 || nearlyEqual(other.y, y2, Box.Precision))
  }

  def nearlyEqual(a: Double, b: Double, epsilon: Double): Boolean = {

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

