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

object DBSCANPoint {

  def fromString(s: String): DBSCANPoint = {
    fromStringArray(s.split(','))
  }

  def fromStringArray(array: Array[String]): DBSCANPoint = {
    DBSCANPoint(array(0).toDouble, array(1).toDouble)
  }

}

case class DBSCANPoint(val x: Double, val y: Double) extends Serializable {

  def this(array: Array[String]) = this(array(0).toDouble, array(1).toDouble)

  def this(s: String) = this(s.split(','))

  def distanceSquared(other: DBSCANPoint) = {
    val dx = other.x - x
    val dy = other.y - y
    dx * dx + dy * dy
  }

}
