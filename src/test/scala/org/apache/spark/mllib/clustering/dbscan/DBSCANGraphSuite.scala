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

class DBSCANGraphSuite extends FunSuite with Matchers  {
  
  
  test("should return connected") {
    
    val graph = DBSCANGraph[Int]().connect(1, 3)
    
    val connected = graph.getConnected(1)
    
    connected should equal(Set(3))
    
  }

  test("should return doubly connected") {
    
    val graph = DBSCANGraph[Int]().connect(1, 3).connect(3, 4)
    
    val connected = graph.getConnected(1)
    
    connected should equal(Set(3, 4))
    
  }

  test("should return none for vertex") {
    
    val graph = DBSCANGraph[Int]().addVertex(5).connect(1, 3)
    
    val connected = graph.getConnected(5)
    
    connected should equal(Set())
    
  }

  test("should return none for unknown") {
    
    val graph = DBSCANGraph[Int]().addVertex(5).connect(1, 3)
    
    val connected = graph.getConnected(6)
    
    connected should equal(Set())
    
  }
}
