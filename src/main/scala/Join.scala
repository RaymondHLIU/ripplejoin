package main.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter
import scala.collection.mutable._
import java.util.{HashMap => JHashMap}

class Join[K: ClassManifest, V: ClassManifest, W: ClassManifest]
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
  var side1Count: Long = 0 //predict count of side1
  var side2Count: Long = 0
  var lastIdx1: Long = 0
  var lastIdx2: Long = 0
  var side1Sum: Long = 0
  var side2Sum: Long = 0
  var approxCount: Long = 0
  var approxSum: Long = 0
  var approxAvg: Double = 0
  var localCount1: Long = 0
  var localCount2: Long = 0
  var countAvg = 0.0
  var countVar1 = 0.0
  var countVar2 = 0.0
  var countVar = 0.0
  var sumVar1 = 0.0
  var sumVar2 = 0.0
  var sumVar = 0.0 
  var avgVar1 = 0.0
  var avgVar2 = 0.0
  var avgVar = 0.0
  
  var rippleResult = List[(K, (V, W))]()
  val resultMap = new JHashMap[K, Array[ArrayBuffer[Any]]]
  
  //val iter1 = rddA.mapPartitions
  val rdd1 = rddA.toArray.iterator         //very cost, I will modify it to do shuffle
  val rdd2 = rddB.toArray.iterator
  val lengthA = rddA.count
  val lengthB = rddB.count
  var idx1: Long = 0
  var idx2: Long = 0
  var side = 2  //side = 2 means element in table 2 will join with sample set from table 1
  var curstep = 1
  
  ///this is output
  val S2 = new PrintWriter("estimate.txt")
  val begintime = System.currentTimeMillis()
  
  var cogroup1 = HashMap[K, Seq[V]]()
  var cogroup2 = HashMap[K, Seq[W]]()
  
  //first layer
  if(rdd1.hasNext && rdd2.hasNext){
    val tmp1 = rdd1.next
    val tmp2 = rdd2.next
    cogroup1.update(tmp1._1, Seq(tmp1._2))
    cogroup2.update(tmp2._1, Seq(tmp2._2))
    //idx1 += 1
    if(tmp1._1 == tmp2._1)
      countAvg = 1
    idx2 += 1
    side = 1
  }
  
  while(rdd1.hasNext && rdd2.hasNext){
    if(side == 2){
      while(1.0*idx1/idx2 > ratioA/ratioB && rdd2.hasNext ){
        //add a new element to cogroup array
        val tmp2 = rdd2.next
        cogroup2.update(tmp2._1, tmp2._2 +: cogroup2.getOrElse(tmp2._1, Seq()))
        val seq1 = cogroup1.getOrElse(tmp2._1, Seq()).iterator
        localCount2 = seq1.length
        while(seq1.hasNext){
          val elem1 = seq1.next
          rippleResult = (tmp2._1, (elem1,tmp2._2)) +: rippleResult
          //statistical info update
          side2Sum += elem1.asInstanceOf[Long] 
       	  side2Sum += tmp2._2.asInstanceOf[Long]          
        }        
        //one side finished
        side2Count += localCount2
        idx2 += 1
        count2 += idx1
        //update statistical info
        //updateApproxSum(side1Sum, side2Sum, idx1, idx2)
        //updateApproxAvg(approxCount, approxSum)
        updateCountVar2(localCount2, idx1)
        //updateSumVar2(side2Sum, idx1)
        //updateSumVar      
      }
      lastIdx2 = idx2
      side = 1
    }else if(side == 1){
      while(1.0*idx1/idx2 <= ratioA/ratioB && rdd1.hasNext){
        //add a new element to cogroup array
        val tmp1 = rdd1.next
        cogroup1.update(tmp1._1, tmp1._2 +: cogroup1.getOrElse(tmp1._1, Seq()))
        val seq2 = cogroup2.getOrElse(tmp1._1, Seq()).iterator
        localCount1 = seq2.length
        while(seq2.hasNext){
          val elem2 = seq2.next
          rippleResult = (tmp1._1, (tmp1._2,elem2)) +: rippleResult
          //statistical info update
          side1Sum += tmp1._2.asInstanceOf[Long]
          side1Sum += elem2.asInstanceOf[Long]
        }
        //one side finished
        side1Count += localCount1 
        idx1 += 1
        count1 += idx2
        //update statistical info
        updateApproxCount(side1Count, side2Count, idx1, idx2)
        //updateApproxSum(side1Sum, side2Sum, idx1, idx2)
        //updateApproxAvg(approxCount, approxSum)
        updateCountVar1(localCount1	, idx2)
        //updateSumVar1(side1Sum, idx2)
        //updateSumVar
      }      
      updateCountVar(idx1,idx2)
      val currentTime = System.currentTimeMillis() - begintime
      S2.println("COUNT: "+ approxCount +" count1: "+ count1 +" side1Count: " + 
            side1Count + " Interval: " + countIterval + "Time: " + currentTime)
      //updateCountAvg(localCount1, localCount2, idx1, idx2)
      lastIdx1 = idx1
      side = 2
    }else{
      println("switch side error")
    }
  }
 
  //statistical info
  def updateApproxCount(count1: Long, count2: Long, idx1: Long, idx2: Long):Unit ={
    approxCount = (count1 + count2) * (lengthA * lengthB) / ((idx1+1) * (idx2+1)) 
  }
  
  def updateApproxSum(joinSum1: Long, joinSum2: Long, a: Long, b: Long):Unit ={
    approxSum = (joinSum1 + joinSum2) * (lengthA * lengthB) / a * b 
  }
  def updateApproxAvg(approxCount: Long, approxSum: Long):Unit ={
    approxAvg = 1.0 * approxSum / approxCount
  }

  //count variance
  //do divide operation only in updateCountVar func
  //update countAvg after update countVar1 or countVar2
  def updateCountVar1(predictCount: Long, side2Passed: Long):Unit ={
    countVar1 += Math.pow((predictCount/(side2Passed+1) - countAvg), 2)  
    countAvg = (1.*(side1Count + side2Count)/((idx1+1)*(idx2+1)))
    println("countAvg: "+countAvg+" countVar: "+countVar+"Iterval: "+countIterval)
    //println((1.*(side1Count + side2Count)/(idx1*idx2))) //verfied, countAvg work well
  }    
  def updateCountVar2(predictCount: Long, side1Passed: Long):Unit ={
    countVar2 += Math.pow((predictCount/(side1Passed+1) - countAvg), 2)
    countAvg = (1.*(side1Count + side2Count)/((idx1+1)*(idx2+1)))
  }
  def updateCountVar(idx1:Long, idx2:Long):Unit={
    countVar = countVar1/(idx1+1) + countVar2/(idx2+1) 
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
    zp * countVar / Math.sqrt(idx2)
  }
  def sumIterval():Double ={
    zp * sumVar / Math.sqrt(idx1 * idx2)
  }
  def avgIterval():Double ={
    zp * avgVar / Math.sqrt(idx1 * idx2)
  }

  
}