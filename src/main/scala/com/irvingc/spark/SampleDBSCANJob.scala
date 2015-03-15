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
package com.irvingc.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf

object SampleDBSCANJob {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("You must pass the arguments: <src file> <dest file> <parallelism>")
    }

    val (src, dest, parallelism) = (args(0), args(1), args(2).toInt)

    val conf = new SparkConf().setAppName("DBSCAN Test")
    val sc = new SparkContext(conf)

    val data = sc.textFile(src)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val labeled = DBSCAN.fit(
      eps = 0.3F,
      minPoints = 10,
      data = parsedData,
      parallelism = parallelism)

    labeled.saveAsTextFile(dest)

  }
}

