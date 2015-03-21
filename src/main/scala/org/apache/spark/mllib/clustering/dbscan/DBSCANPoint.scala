package org.apache.spark.mllib.clustering.dbscan

import archery.Point

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

  def toArcheryPoint(): Point = Point(x.floatValue(), y.floatValue())

}