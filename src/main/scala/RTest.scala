package main.scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.Random
import java.io.PrintWriter

object RTest extends App {

  val sc = new SparkContext("local", "RippleJoinSample",
        System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_RIPPLEJOIN_JAR")))
  
  val RDDSIZE1 = 10000
  val rdd1 = sc.parallelize(0 until 1, 1).flatMap { p =>
      val ranGen = new Random
      var result = List[(Long, Long)]()
      var i: Long = 0
      while (i < RDDSIZE1) {
         result = ((i%(ranGen.nextInt(1000)+1).asInstanceOf[Long]), ranGen.nextInt(10).asInstanceOf[Long]) :: result
          i += 1
          //println(result.toString)
      }
      result
    }
  val RDDSIZE2: Long = 1000
  println(RDDSIZE2)
  val rdd2 = sc.parallelize(0 until 1, 1).flatMap { p =>
    val ranGen = new Random
    var result = List[(Long, Long)]()
    var i :Long = 0
    while (i < RDDSIZE2) {
          result = ((i%(ranGen.nextInt(100)+1).asInstanceOf[Long]), ranGen.nextInt(10).asInstanceOf[Long]) :: result
          i += 1
      }
    result
  }
  val path1 = "/home/liuhao/workspace/rjointest/data/rdd1"
  val path2 = "/home/liuhao/workspace/rjointest/data/rdd2"
  //rdd1.saveAsObjectFile(path1)
  rdd1.saveAsTextFile(path1)
  rdd2.saveAsTextFile(path2)
  println(rdd1.count +" : " + rdd2.count)
  
  //***********************now read from files, do ripple join*********************
  val splitNum = 4
  //now assume text is naturally permauted
  //val rddA = sc.sequenceFile[Long, Int](path1, 2)
  //val rddB = sc.sequenceFile[Long, Int](path2, 2)
  //val rddA = sc.textFile(path1, 2)
  //val rddB = sc.textFile(path2, 2)
  val rddA = rdd1.persist
  val rddB = rdd2.persist
  
  val begintime = System.currentTimeMillis()
  //val ripple = new SimpleJoin(sc, rddA, rddB)
  val ripple = new Join(sc, rddA, rddB)
  val endtime=System.currentTimeMillis()
  val costTime = (endtime - begintime)
  val S = new PrintWriter("test.txt")
  S.println("COUNT: " + ripple.approxCount)
  S.println("Join Time: " + costTime)
  S.close()
  //val ret = ripple.rippleJoin
  val begintime1 = System.currentTimeMillis()
  val ripple1 = rddA.join(rddB)
  val endtime1=System.currentTimeMillis()
  val costTime1 = (endtime1 - begintime1)
  val S1 = new PrintWriter("test1.txt")
  S1.println("COUNT: " + ripple1.count)
  S1.println("Join Time: " + costTime1)
  S1.close()
  
}