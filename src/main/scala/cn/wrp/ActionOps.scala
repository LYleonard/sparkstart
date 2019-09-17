package cn.wrp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description Spark action operations example by Scala
  * * @Author LYleonard
  * * @Date 2019/9/17 9:37
  **/
object ActionOps {

  def main(args: Array[String]): Unit = {
    collectAndCountOps()
  }

  def collectAndCountOps(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      //.setMaster("local[*]") // 使用集群所有节点运行
      .setAppName("collectOps")
    val sc = new SparkContext(conf)

    val filePath = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\wordcount.txt"
    //val filePath = "hdfs://master:8020/user/root/wordcount.txt" // hdfs路径

    val lineRDD = sc.textFile(filePath)
    val wordRDD = lineRDD.flatMap(_.split(" "))
    // 过滤操作，保留“the” 词
    val filteredWordRDD = wordRDD.filter(_.equalsIgnoreCase("the"))

    // action算子 collect，拉取结果到driver节点
    filteredWordRDD.collect()
    val counts = filteredWordRDD.count()

    filteredWordRDD.foreach(println(_))
    println(counts)
  }
}
