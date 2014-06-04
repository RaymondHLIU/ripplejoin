package main.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter
import scala.collection.mutable._
import java.util.{HashMap => JHashMap}

class SimpleJoin[K: ClassManifest, V: ClassManifest, W: ClassManifest]
(sc: SparkContext, rddA: RDD[(K, V)], rddB: RDD[(K, W)]) {

  val ratioA = 10.0   //we assume that user join large table with small table
  val ratioB = 1.0
 
  //statistical info
  val zp = 0.05
  var epson = 0.0
  //var curSum: Long = 0
  //var 
  var count1: Long = 1 //how many tuples passed by side1
  var count2: Long = 0
  var side1Count: Long = 0
  var side2Count: Long = 0
  var side1Sum: Long = 0
  var side2Sum: Long = 0
  var approxCount: Long = 0
  var approxSum: Long = 0
  var approxAvg: Double = 0
  var countVar1 = 0.0
  var countVar2 = 0.0
  var countVar = 0.0
  var sumVar1 = 0.0
  var sumVar2 = 0.0
  var sumVar = 0.0 
  var avgVar1 = 0.0
  var avgVar2 = 0.0
  var avgVar = 0.0
 
  val lengthA = rddA.count
  val lengthB = rddB.count
  
  var rippleResult = List[(K, (V, W))]()
  //val iter1 = rddA.mapPartitions
  val rdd1 = rddA.toArray.iterator         //very cost, I will modify it to do shuffle
  val rdd2 = rddB.toArray.iterator
  var idx1: Long = 0
  var idx2: Long = 0
  var side = 2  //side = 2 means element in table 2 will join with sample set from table 1
  var curstep = 1
  
  ///this is output
  val S2 = new PrintWriter("estimate.txt")
  val begintime = System.currentTimeMillis()
  
  var cogrouparray = HashMap[K, (Seq[V], Seq[W])]()
  //first layer
  if(rdd1.hasNext && rdd2.hasNext){
    val tmp1 = rdd1.next
    val tmp2 = rdd2.next
    cogrouparray.getOrElseUpdate(tmp1._1, (Seq(tmp1._2), Seq(tmp2._2)))
    //idx1 += 1
    idx2 += 1
    side = 1
  }
  
//  def rippleJoin(): RDD[(K, (V, W))]={
//    
//  }
  while(rdd1.hasNext && rdd2.hasNext){
    if(side == 2){
      while(1.0*idx1/idx2 > ratioA/ratioB && rdd2.hasNext ){
        //add a new element to cogroup array
        val tmp2 = rdd2.next
        val tmpcoarray = cogrouparray.get(tmp2._1)
        S2.println("-------"+tmpcoarray.size)
        if(!tmpcoarray.isEmpty){    //insert new element into array
          cogrouparray.update(tmp2._1, (cogrouparray.get(tmp2._1).get._1, 
              cogrouparray.get(tmp2._1).get._2 ++ Seq(tmp2._2)))
        }else{
          cogrouparray.update(tmp2._1, (Seq(), Seq(tmp2._2)))
        }
        //update join result once
        if(!tmpcoarray.isEmpty && !tmpcoarray.get._1.isEmpty){
        	val iter = tmpcoarray.get._1.iterator
       		while(iter.hasNext){
      		  val elementA: V = iter.next
        	  //println(elementA)
        	  rippleResult = (tmp2._1, (elementA, tmp2._2)) :: rippleResult
        	  //statistical info update
        	  side2Count += 1
        	  side2Sum += elementA.asInstanceOf[Long] 
        	  side2Sum += tmp2._2.asInstanceOf[Long]          
        	}
        }
        
        //one side finished
        idx2 += 1
        count2 += idx1
        //update statistical info
        updateApproxCount(side1Count, side2Count, idx1, idx2)
        //updateApproxSum(side1Sum, side2Sum, idx1, idx2)
        //updateApproxAvg(approxCount, approxSum)
        updateCountVar2(side2Count, idx1)
        updateCountVar
        //updateSumVar2(side2Sum, idx1)
        //updateSumVar
        //countIterval        
      }
      side = 1
    }else if(side == 1){
      while(1.0*idx1/idx2 <= ratioA/ratioB && rdd1.hasNext){
        //add a new element to cogroup array
        val tmp1 = rdd1.next
        val tmpcoarray = cogrouparray.get(tmp1._1)
        if(!tmpcoarray.isEmpty){  //insert new element into array
          cogrouparray.update(tmp1._1, (cogrouparray.get(tmp1._1).get._1 ++ Seq(tmp1._2), 
              cogrouparray.get(tmp1._1).get._2 ))    
        }else{
          cogrouparray.update(tmp1._1, (Seq(tmp1._2),Seq()))
        }
        //update join result once
        if(!tmpcoarray.isEmpty && !tmpcoarray.get._1.isEmpty){
          val iter = tmpcoarray.get._2.iterator
          while(iter.hasNext){
            val elementB: W = iter.next
            rippleResult = (tmp1._1, (tmp1._2, elementB)) :: rippleResult
            //statistical info update
            side1Count += 1
            side1Sum += elementB.asInstanceOf[Long] 
            side1Sum += tmp1._2.asInstanceOf[Long] 
          }
        }
        //one side finished
        idx1 += 1
        count1 += idx2
        //update statistical info
        updateApproxCount(side1Count, side2Count, idx1, idx2)
        //updateApproxSum(side1Sum, side2Sum, idx1, idx2)
        //updateApproxAvg(approxCount, approxSum)
        updateCountVar1(side1Count, idx2)
        //updateCountVar
        updateSumVar1(side1Sum, idx2)
        //updateSumVar
        val currentTime = System.currentTimeMillis() - begintime
        if(idx1%10 == 0)
        println("idx1: "+ idx1+" COUNT: "+rippleResult.length+" approxCount: "+ approxCount +" count1: "+ count1 +" side2Count: " + side1Count)
        S2.println("COUNT: "+ approxCount +" count1: "+ count1 +" side1Count: " + side1Count + " Interval: " + countIterval + "Time: " + currentTime)
        S2.println("idx1 "+ idx1 + "idx2: " + idx2 + "result: " + rippleResult.length)
      }
      side = 2
    }else{
      println("switch side error")
    }
    
  }
    
    
  //val iter1 = rddA.iterator(rddA.partition, context)
  //val iter2 = rddB.iterator(rddB.partitions, rddB.)
  //based on partition, each partation changed to a hashmap, then join
// val rddOfHashmaps = rddA.mapPartitions(iterator => {
//   val hashmap = new HashMap[Long, ArrayBuffer[Double]]
//   iterator.foreach { case (key, value)  => hashmap.getOrElseUpdate(key, new ArrayBuffer[Double]) += value
//   Iterator(hashmap)
// }, preserveParitioning = true)
 
  //statistical info
  def updateApproxCount(count1: Long, count2: Long, idx1: Long, idx2: Long):Unit ={
    approxCount = (count1 + count2) * (lengthA * lengthB) / (idx1 * idx2) 
  }
  
  def updateApproxSum(joinSum1: Long, joinSum2: Long, a: Long, b: Long):Unit ={
    approxSum = (joinSum1 + joinSum2) * (lengthA * lengthB) / a * b 
  }
  def updateApproxAvg(approxCount: Long, approxSum: Long):Unit ={
    approxAvg = 1.0 * approxSum / approxCount
  }
  
  //def countVar():Double ={0.0}
  //def sumVar():Double ={0.0}
  //def avgVar():Double ={0.0}
  //count variance
  def updateCountVar1(layerCount: Long, layerLength: Long):Unit ={
    countVar1 = (countVar1 * (count1 - layerLength) +
        layerLength * Math.pow((side1Count/layerLength - approxCount), 2))/count1    
  }
  def updateCountVar2(layerCount: Long, layerLength: Long):Unit ={
    countVar2 = (countVar2 * (count2 - layerLength) +
        layerLength * Math.pow((side2Count/layerLength - approxCount), 2))/count2    
  }
  def updateCountVar(){
    countVar = (side1Count * count1 + side2Count * count2)/(count1 + count2)
  }
  //sum variance
  def updateSumVar2(layerSum: Long, layerLength: Long):Unit ={
    sumVar2 = (sumVar2 * (count2 - layerLength) + 
        layerLength * Math.pow((side2Sum/layerLength - approxAvg), 2))/count2
  }
  def updateSumVar1(layerSum: Long, layerLength: Long):Unit ={
    sumVar1 = (sumVar1 * (count1 - layerLength) + 
        layerLength * Math.pow((side1Sum/layerLength - approxAvg), 2))/count1
  }
  def updateSumVar(){
    sumVar = (sumVar1 * count1+ sumVar2 * count2) / (count1 + count2)
  }
  
  def countIterval():Double ={
    zp * countVar / Math.sqrt(idx1 * idx2)
  }
  def sumIterval():Double ={
    zp * sumVar / Math.sqrt(idx1 * idx2)
  }
  def avgIterval():Double ={
    zp * avgVar / Math.sqrt(idx1 * idx2)
  }

  
}