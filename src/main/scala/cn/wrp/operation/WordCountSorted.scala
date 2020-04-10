package cn.wrp.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description 根据出现次数降序排列WordCount
  * * @Author LYleonard
  * * @Date 2019/9/17 17:25
  **/
object WordCountSorted {
  def main(args: Array[String]): Unit = {
    wordCountSorted()
  }

  def wordCountSorted(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountSorted").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\wordcount.txt")
    val words: RDD[String]= lines.flatMap(x => x.split(" "))
    val wordcounts: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey(_ + _)

    val result: RDD[(Int, String)] = wordcounts.map(count => (count._2, count._1)).sortByKey(false)
//      .map(x => (x._2, x._1))
//    result.foreach(x => println(x._1 + ": " + x._2))
    result.foreach(x => println(x._2 + ": " + x._1))
  }
}
