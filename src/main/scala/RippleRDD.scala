package rjoin

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._

class RippleRDD[K: ClassManifest, V: ClassManifest](rddtable: RDD[(K, V)]) {
  
  val rdd = rddtable.toArray
  var pos = 0
  
  def get(position: Int):(K,V)={
    pos += 1
    rdd(position)
  }
  
  def next():(K,V)={
    pos += 1
    rdd(pos - 1)
  }
  
  def getIndex(position: Int):(K, V)={
    rdd(position)
  }
  def getpos():Int ={
    pos
  }
}