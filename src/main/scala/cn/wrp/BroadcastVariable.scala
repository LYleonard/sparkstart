package cn.wrp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description 共享变量
  * * @Author LYleonard
  * * @Date 2019/9/17 11:51
  **/
object BroadcastVariable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast = sc.broadcast(factor)

    val numberArray = Array(1,2,3,4,5,6)
    val numberRDD = sc.parallelize(numberArray)
    val multiplyNumber = numberRDD.map(num => num * factorBroadcast.value)

    multiplyNumber.foreach(num => println(num))
  }
}
