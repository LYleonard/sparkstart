package cn.wrp.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description 共享变量
  * * @Author LYleonard
  * * @Date 2019/9/17 11:51
  * * 广播变量将一个只读变量发送到每台机器上，不用在任务之间传递变量。
  * * 保证数据一致性，spark只支持broadcast只读变量。
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
