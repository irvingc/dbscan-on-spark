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

class SpaceSplitterSuite extends FunSuite with Matchers {

  test("should find x complement") {

    val box = Box(0, 0, 1, 1)
    val boundary = Box(0, 0, 2, 1)

    val complement = SpaceSplitter.complement(box, boundary)

    complement should equal(Box(1, 0, 2, 1))
  }

  test("should find y complement") {

    val box = Box(0, 0, 2, 1)
    val boundary = Box(0, 0, 2, 2)

    val complement = SpaceSplitter.complement(box, boundary)

    complement should equal(Box(0, 1, 2, 2))
  }

  test("should throw exception on invalid input") {
    val boundary = Box(0, 0, 2, 1)
    val box = Box(0, 0, 2, 2)

    an[IllegalArgumentException] should be thrownBy SpaceSplitter.complement(box, boundary)
  }

  test("should find cost") {
    val section1 = (Box(0, 0, 1, 1), 3)
    val section2 = (Box(2, 2, 3, 3), 4)
    val boundary = new Box(0, 0, 3, 3)

    SpaceSplitter.pointsinBox(boundary, List(section1, section2)) should equal(7)
  }

  test("should find a split") {

    val section1 = (Box(0, 0, 1, 1), 3)
    val section2 = (Box(2, 2, 3, 3), 4)
    val section3 = (Box(0, 1, 1, 2), 2)

    val boundary = new Box(0, 0, 3, 3)

    val (split1, split2, _) = SpaceSplitter.costBasedBinarySplit(boundary, List(section1, section2, section3), 1)

    split1 should equal(Box(0, 0, 1, 3))
    split2 should equal(Box(1, 0, 3, 3))

  }

  test("should return all boxes") {
    val box = Box(0, 0, 3, 3)
    val boxes = SpaceSplitter.allBoxesWith(box, 1)

    val expected = Set(box, Box(0, 0, 1, 3), Box(0, 0, 2, 3), Box(0, 0, 3, 1), Box(0, 0, 3, 2))

    boxes should equal(expected)

  }

  test("should find partitions") {

    val section1 = (Box(0, 0, 1, 1), 3)
    val section2 = (Box(0, 2, 1, 3), 6)
    val section3 = (Box(1, 1, 2, 2), 7)
    val section4 = (Box(1, 0, 2, 1), 2)
    val section5 = (Box(2, 0, 3, 1), 5)
    val section6 = (Box(2, 2, 3, 3), 4)

    val sections = List(section1, section2, section3, section4, section5, section6)

    val partitions = SpaceSplitter.findSplits(sections, 9, 1).map({ case (b, _) => b })

    val expected = List(
      Box(1, 2, 3, 3),
      Box(0, 2, 1, 3),
      Box(0, 1, 3, 2),
      Box(2, 0, 3, 1),
      Box(0, 0, 2, 1))

    partitions should equal(expected)

  }

}
