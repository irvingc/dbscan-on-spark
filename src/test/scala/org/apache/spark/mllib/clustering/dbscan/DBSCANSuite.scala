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

import java.util.regex.PatternSyntaxException

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.Finders
import org.scalatest.FunSuite
import org.scalatest.Matchers

import archery.Point

object DBSCANSuite {

  def toPointLabel(l: LabeledVector) = (l.point, l.label)

  def stringToPointLabel(s: String) = arrayToPointLabel(s.split(','))

  def arrayToPointLabel(array: Array[String]) =
    (Point(array(0).toFloat, array(1).toFloat), array(3).toFloat.intValue())
}

class DBSCANSuite extends FunSuite with MLlibTestSparkContext with Matchers {

  val testData = getClass.getClassLoader.getResource("scaled_data.csv")
  val expectedData = getClass.getClassLoader.getResource("labeled_data.csv")

  test("dbscan") {

    val data = sc.textFile(testData.getFile)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val clustered = DBSCAN.fit(eps = 0.3F, minPoints = 10, data = parsedData, parallelism = 5)

    val labeled = clustered.map(DBSCANSuite.toPointLabel).collectAsMap()

    val expected = sc.textFile(expectedData.getFile)
      .map(DBSCANSuite.stringToPointLabel)
      .collectAsMap()

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

}
