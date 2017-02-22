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

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.Matchers

class DBSCANSuite extends LocalDBSCANArcherySuite with MLlibTestSparkContext with Matchers {

  private val dataFile = "labeled_data.csv"

  private val corresponding = Map(3 -> 2D, 2 -> 1D, 1 -> 3D, 0 -> 0D)

  test("dbscan") {

    val data = sc.textFile(getFile(dataFile).toString())

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))

    val model = DBSCAN.train(parsedData, eps = 0.3F, minPoints = 10, maxPointsPerPartition = 250)
    

    val clustered = model.labeledPoints
      .map(p => (p, p.cluster))
      .collectAsMap()
      .mapValues(x => corresponding(x))

    val expected = getExpectedData(dataFile).toMap

    clustered.size should equal(expected.size)

    clustered.foreach {
      case (key, value) => {
        val t = expected(key)
        if (t != value) {
          println(s"expected: $t but got $value for $key")
        }

      }
    }

    clustered should equal(expected)

  }

}
