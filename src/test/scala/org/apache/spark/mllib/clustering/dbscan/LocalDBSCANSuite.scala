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
import scala.io.Source
import org.apache.spark.mllib.linalg.Vectors
import java.net.URI
import org.scalatest.Matchers

class LocalDBSCANSuite extends FunSuite with Matchers {

  val corresponding = Map(1 -> 2, 3 -> 4, 2 -> 1, Noise -> Noise)

  test("should cluster") {

    val labeled = new LocalDBSCAN(eps = 0.3F, minPoints = 10)
      .fit(getRawData("scaled_data.csv"))
      .map(l => (l.point, corresponding(l.localLabel)))
      .toMap

    val expected = getExpectedData("labeled_data.csv")

    labeled.foreach {
      case (key, value) => {
        val t = expected(key)
        if (t != value) {
          println(s"expected: $t but got $value for $key")
        }

      }
    }

    labeled should equal(expected)

  }

  def getExpectedData(file: String) = {
    Source
      .fromFile(getFile(file))
      .getLines()
      .map(DBSCANSuite.stringToPointLabel)
      .toMap
  }

  def getRawData(file: String): List[LabeledVector] = {

    Source
      .fromFile(getFile(file))
      .getLines()
      .map(s => {

        val vector = Vectors.dense(s.split(',').map(_.toDouble))
        new LabeledVector(DBSCAN.toProjection(vector), 1, MarginClassifier.Belonging)

      })
      .toList

  }
  def getFile(filename: String): URI = {
    getClass.getClassLoader.getResource(filename).toURI
  }

}
