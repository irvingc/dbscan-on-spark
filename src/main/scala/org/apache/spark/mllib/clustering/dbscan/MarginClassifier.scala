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
import archery.Point
import archery.Point

object MarginClassifier extends Enumeration {

  type Class = Value
  val Inner, Outer, Belonging, NotBelonging = Value

  def classify(dbscanPoint: DBSCANPoint, margins: Margins): Class =
    classify(dbscanPoint.toArcheryPoint(), margins)

  def classify(point: Point, margins: Margins): Class = margins match {
    case (inner, main, outer) => {
      if (inner.contains(point)) {
        Belonging
      } else if (main.contains(point)) {
        Inner
      } else if (outer.contains(point)) {
        Outer
      } else {
        NotBelonging
      }
    }
  }
  def isInterior(classifier: Class) : Boolean = {
    classifier == Inner || classifier == Belonging
    
  }
}
