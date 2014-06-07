package main.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter
import scala.collection.mutable._
import java.util.{HashMap => JHashMap}

class RippleJoin[K: ClassManifest, V: ClassManifest, W: ClassManifest]
(sc: SparkContext, rddA: RDD[(K, V)], rddB: RDD[(K, W)]) {

  val ratioA = 10.0   //we assume that user join large table with small table
  val ratioB = 1.0
 
  //statistical info
  val zp = 0.05
  var epson = 0.0
  
  var approxAvg: Double = 0
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
  val estimator = new CountEstimator(lengthA, lengthB, zp)
  val sumer = new SumEstimator(lengthA, lengthB, zp)
  val avger = new AvgEstimator(lengthA, lengthB, zp)
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
    if(tmp1._1 == tmp2._1){
      estimator.countAvg = 1
      sumer.sumAvg += (tmp1._2.asInstanceOf[Double] + tmp2._2.asInstanceOf[Double])
    }
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
        val localCount = seq1.length
        var localSum = tmp2._2.asInstanceOf[Long] * localCount
        while(seq1.hasNext){
          val elem1 = seq1.next
          rippleResult = (tmp2._1, (elem1,tmp2._2)) +: rippleResult
          //statistical info update
          localSum += elem1.asInstanceOf[Long]          
        }        
        //one side finished
        estimator.updateSide2(localCount,idx1,idx2)
        sumer.updateSide2(localSum, idx1, idx2)
        avger.updateSide2(localSum/idx1, localCount/idx1, sumer.sumAvg, estimator.countAvg)
        idx2 += 1
      }
      side = 1
    }else if(side == 1){
      while(1.0*idx1/idx2 <= ratioA/ratioB && rdd1.hasNext){
        //add a new element to cogroup array
        val tmp1 = rdd1.next
        cogroup1.update(tmp1._1, tmp1._2 +: cogroup1.getOrElse(tmp1._1, Seq()))
        val seq2 = cogroup2.getOrElse(tmp1._1, Seq()).iterator
        val localCount = seq2.length
        var localSum = tmp1._2.asInstanceOf[Long] * localCount
        while(seq2.hasNext){
          val elem2 = seq2.next
          rippleResult = (tmp1._1, (tmp1._2,elem2)) +: rippleResult
          //statistical info update
          localSum += elem2.asInstanceOf[Long]
        }
        //one side finished,update statistical info
        estimator.updateSide1(localCount,idx1,idx2)
        sumer.updateSide1(localSum, idx1, idx2)
        avger.updateSide1(localSum/idx2, localCount/idx2, sumer.sumAvg, estimator.countAvg, 
            sumer.sumVar, estimator.countVar, sumer.approxSum, estimator.approxCount, idx1, idx2)
        idx1 += 1
      }      
      //estimator.updateCountVar(idx1,idx2)
      val currentTime = System.currentTimeMillis() - begintime
      //if(Math.random()<0.1)
      //S2.println("COUNT: "+ estimator.approxCount +" count1: "+ estimator.count1 +" predicate1: " + 
        //    estimator.predicate1 + " Interval: " + estimator.interval(idx2) + "Time: " + currentTime)
      //S2.println("SUM: "+ sumer.approxSum +" sum1: "+ sumer.count1 +" predicate1: " + 
        //    sumer.predicate1 + " Interval: " + sumer.interval(idx2) + "Time: " + currentTime)
      S2.println("AVG: "+ avger.approxAvg +" Var: " + avger.avgVar + " rho: "+ avger.rho +" Interval: " + 
          avger.interval(idx2) + "Time: " + currentTime)
      side = 2
    }else{
      println("switch side error")
    }
  }
 
  //statistical info
  def updateApproxAvg(approxCount: Long, approxSum: Long):Unit ={
    approxAvg = 1.0 * approxSum / approxCount
  }
  def avgIterval():Double ={
    zp * avgVar / Math.sqrt(idx1 * idx2)
  }

  
}