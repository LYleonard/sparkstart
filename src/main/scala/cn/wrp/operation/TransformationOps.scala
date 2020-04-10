package cn.wrp.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description Spark transformation operations example by Scala
  * * @Author LYleonard
  * * @Date 2019/9/16 16:42
  **/
object TransformationOps {

  val conf = new SparkConf().setAppName("TransformationOps").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //mapOperation()
    //filterOperation()
    //flatMapOperation()
    //groupByKeyOps()
    //reduceByKeyOps
    //sortByKeyOps()
    //joinOps()
    cogroupOps()
  }

  def mapOperation(): Unit = {
    val numbers = List(1, 2, 3, 4, 5)
    val numbersRDD = sc.parallelize(numbers,1)
    val resultRDD = numbersRDD.map(x => x * 2)

    resultRDD.foreach(x => println(x))
  }

  def filterOperation(): Unit = {
    val numbers = List(1, 2, 3, 4, 5)
    val numbersRDD = sc.parallelize(numbers,1)
    val resultRDD = numbersRDD.filter(x => x % 2 == 0)

    resultRDD.foreach(x => println(x))
  }

  def flatMapOperation(): Unit = {
    val arrayStr = Array("Spark Transformation", "is a function", "that produces new RDD from the existing RDDs")
    val arrayStrRDD = sc.parallelize(arrayStr,1)
    val resultRDD = arrayStrRDD.flatMap(x => x.split(" "))

    resultRDD.foreach(x => println(x))
  }

  def groupByKeyOps(): Unit = {
    val scores = Array(
      Tuple2("class1", 85),
      Tuple2("class2", 75),
      Tuple2("class1", 98),
      Tuple2("class2", 88)
    )

    val scoresRDD = sc.parallelize(scores, 1)
    val groupScores = scoresRDD.groupByKey()
    groupScores.foreach(x => {
      println(x._1 + ":");
      x._2.foreach(y => println(y))
    })
  }

  def reduceByKeyOps(): Unit = {
    val scores = Array(
      Tuple2("class1", 85),
      Tuple2("class2", 75),
      Tuple2("class1", 98),
      Tuple2("class2", 88)
    )

    val scoresRDD = sc.parallelize(scores, 1)
    val reduceScores = scoresRDD.reduceByKey(_ + _)
    reduceScores.foreach(x => println(x._1 + ": " + x._2))

  }

  def sortByKeyOps(): Unit = {
    val scores = Array(
      Tuple2(85, "lee"),
      Tuple2(98, "marry"),
      Tuple2(75, "Tom"),
      Tuple2(85, "jerry")
    )
    val sortRdd = sc.parallelize(scores, 1)
    val result  = sortRdd.sortByKey(false)

    result.foreach(x => {
      println(x._1 + ": " + x._2)
    })
  }

  def joinOps(): Unit = {
    val studentsList = Array(
      Tuple2(1, "lee"),
      Tuple2(2, "Jack"),
      Tuple2(3, "Tom"))
    val scoresList = Array(
      Tuple2(1, 98),
      Tuple2(2, 75),
      Tuple2(3, 87))

    val studentsRDD = sc.parallelize(studentsList)
    val scoresRDD = sc.parallelize(scoresList)
    val result = studentsRDD.join(scoresRDD)

    result.foreach(x => {
      println("studentID: " + x._1)
      println("studentName: " + x._2._1)
      println("studentScores: " + x._2._2)
      println("==========================")
    })

  }
  def cogroupOps(): Unit = {
    val studentsList = Array(
      Tuple2(1, "lee"),
      Tuple2(2, "Jack"),
      Tuple2(3, "Tom"))
    val scoresList = Array(
      Tuple2(1, 98),
      Tuple2(2, 75),
      Tuple2(3, 87),
      Tuple2(1, 97),
      Tuple2(2, 85),
      Tuple2(3, 77))

    val studentsRDD = sc.parallelize(studentsList)
    val scoresRDD = sc.parallelize(scoresList)
    val result = studentsRDD.cogroup(scoresRDD)

    result.foreach(x => {
      println("studentID: " + x._1)
      println("studentName: " + x._2._1)
      println("studentScores: " + x._2._2)
      println("=====================================")
    })

  }
}
