package cn.wrp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Author LYleonard
  * * @Date 2019/9/18 15:31
  * * @Description scala版二次排序
  **/
object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\sort.txt")
    val pair: RDD[(SecondarySortKey, String)] = lines.map(line => {
      val cols = line.split(" ")
      val key: SecondarySortKey = new SecondarySortKey(cols(0).toInt, cols(1).toInt)
      (key, line)
    })

    val sortPairs = pair.sortByKey().map(sortPair => sortPair._2)
    sortPairs.foreach(sortLine => println(sortLine))
  }
}
