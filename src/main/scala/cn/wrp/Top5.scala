package cn.wrp

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author LYleonard
  * * @Date 2019/9/18 22:20
  * * @Description 用RDD的sortByKey()实现Top5
  **/
object Top5 {

  def main(args: Array[String]): Unit = {
    //DataFrameOrderBy()
    sortByKeyTop5()
  }

  def DataFrameOrderBy(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataFrameOrderBy").setMaster("local")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换, $"columnName"表示列
    import sparkSession.implicits._

    val url = "jdbc:mysql://localhost:3306/band?characterEncoding=utf8&useSSL=false"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "243015")

    val ordersDF = sparkSession.read.jdbc(url, "orders", prop)
    val ordersTotal = ordersDF.select($"total")
    val top5 = ordersTotal.orderBy($"total".desc).take(5)

    import org.apache.spark.sql.functions._
    val top3 = ordersTotal.orderBy(desc("total")).take(3)

    top5.foreach(x => println(x(0)))
    println("------------------------------")
    top3.foreach(x => println(x(0)))
  }

  def sortByKeyTop5(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sortByKeyTop5").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)

    val list = Array(169,288,132,176,165,173,226,293,129,148)
    val nums = sc.parallelize(list)

    val numPairs = nums.map(num => (num, num))
    val top5 = numPairs.sortByKey(false).take(5)

    for(num <- top5){
      println(num._2)
    }
  }
}
