package cn.wrp.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/4/10 20:49
  * @Description TODO
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

//    val lines = sc.textFile("hdfs://master:9000/spark.txt", 1)
    val lines = sc.textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\wordcount.txt", 1)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    val counts = lines.flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey(_ + _)
      .saveAsTextFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\wordcountResult")

    wordCounts.foreach(wordCount => println(wordCount._1 + ":" + wordCount._2))

  }
}
