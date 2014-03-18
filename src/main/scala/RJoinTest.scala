package rjoin

import org.apache.spark._
import org.apache.spark.rdd.RDD
import SparkContext._
import java.util.Random
import java.io.PrintWriter

object RJoinTest {

  def main(args: Array[String]): Unit = {  
    test()  
  } 
  def test(): Unit = {  
    //val a1 = Array((111,"ass"),(222,"dcc"),(333,"rff"))
    //val b1 = Array((111,"afd"),(222,"fsd"),(333,"vfv"))
 
    val b1 = List((111,121),(222,1231),(333,53453),(444,244),(555,56),(666,67),(777,78),(888,90))
    val a1 = List((111,191),(222,16),(444,34),(666,678),(888,235))
    
    val sc = new SparkContext("local", "RippleJoinSample",
        System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_RIPPLEJOIN_JAR")))
    //val rdd1 = sc.parallelize(a1, 1)
    //val rdd2 = sc.parallelize(b1, 1)
    val rdd1 = sc.makeRDD(a1)
    val rdd2 = sc.makeRDD(b1)
    
    /*
     
     val rdd1 = sc.parallelize(0 until 2, 2).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, String)](1000)
      for (i <- 0 until 1000) {
          result(i) = (i, ranGen.toString())
      }
      result
    }
    val rdd2 = sc.parallelize(0 until 2, 2).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, String)](1000)
      for (i <- 0 until 1000) {
          result(i) = (i, ranGen.toString())
      }
      result
    }
    */
    
    //how tranfer rdd1 to rippleRDD
    //val rj = new RJoin(new RippleRDD(sc, rdd1), new RippleRDD(sc, rdd2)) 
    val rj = new RJoin(sc, rdd1, rdd2) 
    val ret = rj.rippleJoin()  
    //println(list1)
    //println(list2)
    //val list = (tableA.get(1), tableB.get(1))
    //ret.saveAsTextFile("output")
    val S = new PrintWriter("test.txt")
    S.println(rdd1)
    S.println(ret.collect.mkString)
    //val arr = rdd1.collect.mkString
    //S.println(arr.toString)
    S.println(ret.count)
    S.close()
    //ret.saveAsTextFIle(output)
    //for (i <- (0 to 6)) println(ret(i))  
    //ret.foreach((i:Int)=>println(ret(i)))
  } 
  
}