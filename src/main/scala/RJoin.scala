package mian.scala
import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter

class RJoin[K: ClassManifest, V: ClassManifest, W: ClassManifest]
(sc: SparkContext, rddA: RDD[(K, V)], rddB: RDD[(K, W)]) {

  val zp = 0.95
  var epson = 0.0
  var beta1 = 1
  var beta2 = 4000
  
  var side = 2 //init side is left
  var pos1 = 0 //current cursor in table 1
  var pos2 = 0
  var maxpos1 = 0
  var maxpos2 = 1
  var layerfinished1 = 0
  var layerfinished2 = 0
  
  var eof1 = false
  var eof2 = false
  var curstep = 1
  
  //count
  var elementCounter = 0
  var predictCounter = 0
  var sideElementCounter = 0
  var sidePredictCounter = 0
  var countAvg1 = 0.0
  var countAvg2 = 0.0
  var countAvg = 0.0
  var estimateCountVar1 = 0.0
  var estimateCountVar2 = 0.0
  var estimateCountVar = 0.0
  
  val rdd1 = rddA.toArray
  val rdd2 = rddB.toArray
  val length1 = rdd1.length - 1
  val length2 = rdd2.length - 1
  
  var newList: List[(K, (V, W))] = Nil//new Array[(K, (V, W))]((rdd1.length * rdd2.length).toInt)
 
  def rippleJoin(): RDD[(K,(V, W))] = {
    val S2 = new PrintWriter("estimate.txt")
    val begintime = System.currentTimeMillis()
    
    if(length1 > 0 && length2 > 0 ){  //init, first layer
      predict(pos1, pos2)
      pos2 = 1
      maxpos1 = 1
      maxpos2 = 1
      layerfinished1 = 1
      layerfinished2 = 1
      curstep = 2
      side = 1
      //init estimator
      elementCounter += 1
      predictCounter += sidePredictCounter
      //countAvg2 = sidePredictCounter
      countAvg = sidePredictCounter
      //sumAvg2 = sideSum2
    }
    while(layerfinished1 < length1 || layerfinished2 < length2){
      //if()
      if(side == 2){
        if(layerfinished1 >= length1)  side = 1  //if this is the smaller side and finished, 
        										//only loop another side
        pos2 = 0
        pos1 = maxpos1
        
        while( (maxpos1 * 1.0)/maxpos2 <= (beta1 * 1.0)/beta2 ){  //make sure Non-unitary aspect ratio is right
          //for estimator, clean
          sideElementCounter = 0
          sidePredictCounter = 0
          //sideSum2 = 0.0
          while(pos2 < maxpos2 && !eof1 && !eof2){
            if(pos1 < length1 && pos2 < length2){
              predict(pos1, pos2)
              sideElementCounter += 1
              //println(elementCounter+"  ",side+": ",  pos1+"<="+maxpos1, pos2+"<="+maxpos2)
              pos2 += 1
            }else{
              eof2 = true
            }
          }        
          //for estimator, compute side estimate
          elementCounter += sideElementCounter
          predictCounter += sidePredictCounter
          countAvg2 = (countAvg * maxpos1 + sidePredictCounter)/(maxpos1 + 1)
          estimateCountVar2 = (estimateCountVar2 * maxpos1 + Math.pow((sidePredictCounter - countAvg2), 2)) / (maxpos1 + 1)
          //val sideSumAvg = if(sidePredictCounter > 0) (sideSum2 * 1.0)/sidePredictCounter else 0
          //sum2 += sideSum2
          
          pos1 += 1
          maxpos1 += 1
          pos2 = 0
        }
        if(maxpos1 > layerfinished1)
          layerfinished1 = maxpos1
          
        //for estimator, compute final estimate 
        estimateCountVar = estimateCountVar1 / (layerfinished2 * beta1) +
                           estimateCountVar2 / (layerfinished1 * beta2)
        
        curstep += 1
        side = 1
        val currentTime = System.currentTimeMillis() - begintime
        S2.println("COUNT: "+ countAvg + "Interval: " + countEpson() + "Time: " + currentTime)
      } else{
        if(layerfinished2 >= length2)  side = 2  //if this is the smaller side and finished, 
        										//only loop another side
        pos2 = maxpos2
        pos1 = 0
        
        while((maxpos1 * 1.0)/maxpos2 > (beta1 * 1.0)/beta2){    
          //for estimator, clean
          sideElementCounter = 0
          sidePredictCounter = 0
          //sideSum1 = 0.0
          
          while(pos1 < maxpos1 && !eof1 && !eof2){
            if(pos1 <= length1 && pos2 <= length2){
              predict(pos1, pos2)
              elementCounter += 1
              //println(elementCounter+"  ",side+": ", pos1+"<="+maxpos1, pos2+"<="+maxpos2)
              pos1 += 1
              //println(newList)
            }else{
              eof1 = true
            }
          }   
           //for estimator, compute side estimate
          elementCounter += sideElementCounter
          predictCounter += sidePredictCounter
          countAvg1 = (countAvg * maxpos2 + sidePredictCounter)/(maxpos2 + 1)
          estimateCountVar1 = (estimateCountVar1 * maxpos2 + Math.pow((sidePredictCounter - countAvg1), 2)) / (maxpos2 + 1)
          //val sideSumAvg = if(sidePredictCounter > 0) (sideSum1 * 1.0)/sidePredictCounter else 0
          //sum1 += sideSum1
          
          pos2 += 1
          maxpos2 += 1
          pos1 = 0
          //println(maxpos1,maxpos2,"  "+(maxpos1 * 1.0)/maxpos2,"  " , (beta1 * 1.0)/beta2)
        }
       
        if(maxpos2 > layerfinished2)
          layerfinished2 = maxpos2
        side = 2
      }
    }
    
    curstep = 1;
    side = 2 
  
    S2.close()
    sc.makeRDD(newList)
  }
  
  def predict(pos1: Int, pos2: Int){
    if(rdd1(pos1)._1 == rdd2(pos2)._1){//need optimize if stream, perhaps use !eof1
              val r1 = rdd1(pos1)._2
              val r2 = rdd2(pos2)._2
              newList = (rdd1(pos1)._1, (r1, r2)) :: newList
              //for estimator
              sidePredictCounter += 1
    }
  }
  //some util function
  def pos(side: Int):Int ={
    if(side == 2)
      pos2
      else pos1
  } 

  def eof(side: Int):Boolean ={
    if(side == 2)
      eof2
      else
        eof1
  }

  def length(side: Int):Int ={
    if(side == 2)
      length2
      else length1
  }
  
  
  /*
   * some operators
   */
  def count():Int ={
    val ret = (((length1 * length2 * 1.0)/elementCounter) * predictCounter).toInt
    ret
  }
  
  def countVar(): Double ={
    val ret = estimateCountVar1/beta1 + estimateCountVar2/beta2
    ret
  }
  
  def countEpson(): Double ={
    zp * estimateCountVar / Math.sqrt(elementCounter)
  }
  
  //def updateSideSum(r1: Double, r2: Double){
  //    if(side == 2)
  //      sideSum2 += r1 + r2
  //      else
  //        sideSum1 += r1 + r2
  //}
  //def sum():Double ={
  //  val ret = ((length1 * length2  * 1.0)/ elementCounter) * (sum1 + sum2)
  // ret
  //}
  //def avg():Double={
  //  if(count()>0)
  //    sum()/count()
  //    else
  //      0.0
  //}
}