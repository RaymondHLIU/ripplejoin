package rjoin

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter

//import java.util.logging.Logger;
//import org.slf4j.LoggerFactory;

class RJoin[K: ClassManifest, V: ClassManifest, W: ClassManifest]
(sc: SparkContext, rddA: RDD[(K, V)], rddB: RDD[(K, W)]) {

  var beta1 = 1
  var beta2 = 2
  
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
  var counter = 0
    
  val rdd1 = rddA.toArray
  val rdd2 = rddB.toArray
  val length1 = rdd1.length
  val length2 = rdd2.length
  
  var newList: List[(K, (V, W))] = Nil//new Array[(K, (V, W))]((rdd1.length * rdd2.length).toInt)
 
  def rippleJoin(): RDD[(K,(V, W))] = {
    
    if(length1 > 0 && length2 > 0 ){  //init, first layer
      println(pos1, pos2)
      pos2 = 1
      maxpos1 = 1
      maxpos2 = 1
      layerfinished1 = 1
      layerfinished2 = 1
      curstep = 2
      side = 1
    }
    while(layerfinished1 < length1 || layerfinished2 < length2){
      if(side == 2){
        if(layerfinished1 >= length1)  side = 1  //one side all finished, only loop other side
        pos2 = 0
        pos1 = maxpos1
        while( (maxpos1 * 1.0)/maxpos2 <= (beta1 * 1.0)/beta2 ){  //make sure ratio is right, not restrict step
          while(pos2 < maxpos2){
            //if(pos1 < length1 && pos2 < length2){
              predict(pos1, pos2)
            counter += 1
            println(counter+"  ",side+": ",  pos1+"<="+maxpos1, pos2+"<="+maxpos2)
            pos2 += 1
            //}
          }
          pos1 += 1
          maxpos1 += 1
          pos2 = 0
        }
        if(maxpos1 > layerfinished1)
          layerfinished1 = maxpos1
        curstep += 1
        side = 1
      } else{
        if(layerfinished2 >= length2)  side = 2
        pos2 = maxpos2
        pos1 = 0
        while((maxpos1 * 1.0)/maxpos2 > (beta1 * 1.0)/beta2){
          while(pos1 < maxpos1){
            //if(pos1 <= rdd1.length && pos2 <= rdd2.length){
              predict(pos1, pos2)
            counter += 1
            println(counter+"  ",side+": ", pos1+"<="+maxpos1, pos2+"<="+maxpos2)
            pos1 += 1
              //println(newList)
            //}
          }
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
  
    sc.makeRDD(newList)
  }
  
  def predict(pos1: Int, pos2: Int){
    if(pos1 < length1 && pos2 < length2
                && rdd1(pos1)._1 == rdd2(pos2)._1){//need optimize if stream, perhaps use !eof1
              val r1 = rdd1(pos1)._2 
              val r2 = rdd2(pos2)._2
              newList = (rdd1(pos1)._1, (r1, r2)) :: newList
              println(newList)
    }
  }
  //some util function
  def pos(side: Int):Int ={
    if(side == 2)
      pos2
      else pos1
  } 
  def repos(side: Int){
    if(side == 2)
      pos2 = 0
      else
        pos1 = 0
  }
 
  def eof(side: Int):Boolean ={
    if(side == 2)
      eof2
      else
        eof1
  }
  def stateof(side: Int){
    if(side == 2)
      eof2 = true
      else
        eof1 = true
  }
  def length(side: Int):Int ={
    if(side == 2)
      length2
      else length1
  }
}
