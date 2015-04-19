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

import org.scalatest.FunSuite
import org.scalatest.Matchers

class EvenSplitPartitionerSuite extends FunSuite with Matchers {
  test("should find partitions") {

    val section1 = (DBSCANRectangle(0, 0, 1, 1), 3)
    val section2 = (DBSCANRectangle(0, 2, 1, 3), 6)
    val section3 = (DBSCANRectangle(1, 1, 2, 2), 7)
    val section4 = (DBSCANRectangle(1, 0, 2, 1), 2)
    val section5 = (DBSCANRectangle(2, 0, 3, 1), 5)
    val section6 = (DBSCANRectangle(2, 2, 3, 3), 4)

    val sections = Set(section1, section2, section3, section4, section5, section6)

    val partitions = EvenSplitPartitioner.partition(sections, 9, 1)

    val expected = List(
      (DBSCANRectangle(1, 2, 3, 3), 4),
      (DBSCANRectangle(0, 2, 1, 3), 6),
      (DBSCANRectangle(0, 1, 3, 2), 7),
      (DBSCANRectangle(2, 0, 3, 1), 5),
      (DBSCANRectangle(0, 0, 2, 1), 5))

    partitions should equal(expected)

  }

  test("should find two splits") {

    val section1 = (DBSCANRectangle(0, 0, 1, 1), 3)
    val section2 = (DBSCANRectangle(2, 2, 3, 3), 4)
    val section3 = (DBSCANRectangle(0, 1, 1, 2), 2)

    val sections = Set(section1, section2, section3)

    val partitions = EvenSplitPartitioner.partition(sections, 4, 1)

    partitions(0) should equal((DBSCANRectangle(1, 0, 3, 3), 4))
    partitions(1) should equal((DBSCANRectangle(0, 1, 1, 3), 2))

  }
}
