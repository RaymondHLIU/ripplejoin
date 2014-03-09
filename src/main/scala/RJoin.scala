package rjoin

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.io.PrintWriter

//import java.util.logging.Logger;
//import org.slf4j.LoggerFactory;

class RJoin[K: ClassManifest, V: ClassManifest, W: ClassManifest]
(sc: SparkContext, rddA: RDD[(K, V)], rddB: RDD[(K, W)]) {

  var curstep = 1
  var side = 2 
  var pos1 = 1
  var pos2 = 0
    
  val rdd1 = rddA.toArray
  val rdd2 = rddB.toArray
  
  def joinBside(curstep: Int):Array[(K, (V, W))] ={
    var ret = new Array[(K, (V, W))](curstep)
    while(pos2 < curstep){
      if(pos1 <= rdd1.length && pos2 <= rdd2.length){
        pos2 += 1
        if(rdd1(pos1 - 1)._1 == rdd2(pos2 - 1)._1){
          val r1 = rdd1(pos1 - 1)._2 
          val r2 = rdd2(pos2 - 1)._2
          //merge (TableA.list.index(pos), TableB.list.index(pos))
          ret(pos2 - 1) = (rdd1(pos1 - 1)._1, (r1, r2))
        }
      //println(ret)
      }
    }
    ret
  }
  
  def joinAside(curstep: Int):Array[(K, (V, W))] ={
    var ret = new Array[(K, (V, W))](curstep)
    while(pos1 < curstep - 1){
      if(pos1 <= rdd1.length && pos2 <= rdd2.length){
        pos1 += 1
        if(rdd1(pos1 - 1)._1 == rdd2(pos2 - 1)._1){          
          val r1 = rdd1(pos1 - 1)._2
          val r2 = rdd2(pos2 - 1)._2
          //merge (TableA.list.index(pos), TableB.list.index(pos))
          ret(pos1 - 1) = (rdd2(pos2 - 1)._1, (r1, r2))
          
        }
        //println(ret)
      }
    }
    ret
  }
  
  def rippleJoin(): RDD[(K,(V, W))] = {
    
    var newarr = List()//new Array[(K, (V, W))]((rdd1.length * rdd2.length).toInt)
    var i = 0
    //var counter = 0
    while(curstep <= rdd1.length){
      if(side == 2){
        //newarr ++ joinBside(curstep)
        {
          while(pos2 < curstep){
            if(pos1 <= rdd1.length && pos2 <= rdd2.length){
            pos2 += 1
            if(rdd1(pos1 - 1)._1 == rdd2(pos2 - 1)._1){
              val r1 = rdd1(pos1 - 1)._2 
              val r2 = rdd2(pos2 - 1)._2
              //merge (TableA.list.index(pos), TableB.list.index(pos))
              //newarr(counter) = (rdd1(pos1 - 1)._1, (r1, r2))
              //counter += 1
              newarr.::( (rdd1(pos1 - 1)._1, (r1, r2)) )
            }
           //println(ret)
            }
          }
        }
        side = 1
        curstep += 1
        pos1 = 0
      } else{
        //newarr ++ joinAside(curstep)
        {
          while(pos1 < curstep - 1){
            if(pos1 <= rdd1.length && pos2 <= rdd2.length){
              pos1 += 1
              if(rdd1(pos1 - 1)._1 == rdd2(pos2 - 1)._1){          
                val r1 = rdd1(pos1 - 1)._2
                val r2 = rdd2(pos2 - 1)._2
                //merge (TableA.list.index(pos), TableB.list.index(pos))
                //newarr(counter) = (rdd2(pos2 - 1)._1, (r1, r2))
                //counter += 1
                newarr.::( List((rdd2(pos2 - 1)._1, (r1, r2))))
              }
            //println(ret)
            }
          }
        }
        pos1 += 1
        side = 2
        pos2 = 0
      }
    }
    curstep = 1;
    side = 2 
    
    val S = new PrintWriter("tests.txt")
    S.println(newarr.length)
    S.println(newarr.mkString)
    S.close()
  
    sc.makeRDD(newarr)
  }
}
