package cn.wrp.operation

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * * @Author LYleonard
  * * @Date 2019/9/18 22:20
  * * @Description 用RDD的sortByKey()实现Top5
  **/
object TopN {

  def main(args: Array[String]): Unit = {

//    val num: Int = args(0).toInt  //TopN的值，参数传递
//    val filepath: String = args(1)  //读取文件路径

    val num: Int = 4  //TopN的值，参数传递
    val uniqueTopNpath: String = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\uniqueTopN.txt"
    val nonUniqueTopNpath: String = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\nonUniqueTopN.txt"
    val groupTopNpath: String = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\groupTopN.txt"

//    sparkUniqueTopN(num, uniqueTopNpath)
    sparkNonUniqueTopN(num, nonUniqueTopNpath)
//    sparkGroupTopN(num,groupTopNpath)
    //DataFrameOrderBy()
    //sortByKeyTop5()
  }

  /**
    * 1.唯一键的TopN算法，就是Key的值是唯一的；
    * @param num TopN 的N值
    * @param filepath 读取文件的路径
    */
  def sparkUniqueTopN(num: Int, filepath: String): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local").setAppName("SparkUniqueTopN")  // Spark配置
    val sparkContext: SparkContext = SparkSession.builder().config(config).getOrCreate().sparkContext //构建Spark上下文

    val topN: Broadcast[Int] = sparkContext.broadcast(num) //广播变量
    val linesRdd: RDD[String] = sparkContext.textFile(filepath)
    val pairRdd: RDD[(Int, Array[String])] = linesRdd.map(line => {
      val tokens: Array[String] = line.split(" ")
      (tokens(1).toInt, tokens)
    })

    // 1.首先，在每个分区内计算TopN
    val partitions: RDD[(Int, Array[String])] = pairRdd.mapPartitions(iterator => {
      var sortedMap = SortedMap.empty[Int, Array[String]]
      iterator.foreach(tuple => {
        sortedMap += tuple
        if (sortedMap.size > topN.value) {
          sortedMap = sortedMap.takeRight(topN.value)
        }
      })
      sortedMap.takeRight(topN.value).toIterator
    })

    // 2.规约，将各分区TopN拉取到driver内存
    val alltopN: Array[(Int, Array[String])] = partitions.collect()

    // 3.对各分区TopN执行TopN
    val finaltopN: SortedMap[Int, Array[String]] = SortedMap.empty[Int, Array[String]].++:(alltopN)
    val resultUsingMapPartition: SortedMap[Int, Array[String]] = finaltopN.takeRight(topN.value)

    println("------------ SortedMap计算TopN的结果：--------------")
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }

    // 方法2：采用RDD的sortByKey方法计算TopN
    val moreConciseApproach: Array[(Int, Iterable[Array[String]])] = pairRdd
      .groupByKey()
      .sortByKey(ascending = false).take(topN.value)
    println("------------ sortByKey方法计算TopN的结果：--------------")
    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.flatten.mkString(",")}")
    }
  }

  /**
    * 2.非唯一键的TopN算法，比如key值可能会有多个A、B、C，然后分别对他们求和后计算TopN；
    * @param num TopN 的N值
    * @param filepath 读取文件的路径
    */
  def sparkNonUniqueTopN(num: Int, filepath: String): Unit ={

    val config: SparkConf = new SparkConf().setMaster("local").setAppName("SparkNonUniqueTopN")
    val sparkContext: SparkContext = SparkSession.builder().config(config).getOrCreate().sparkContext  //构建Spark上下文

    val topN: Broadcast[Int] = sparkContext.broadcast(num)  //广播变量
    val linesRdd: RDD[String] = sparkContext.textFile(filepath).filter(line => line.length > 0)  //过滤空行
    val kv: RDD[(String, Int)] = linesRdd.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toInt)
    })
    val uniqueKeys: RDD[(String, Int)] = kv.reduceByKey(_ + _)  //将非唯一键转换为唯一键

    // 方法1
    val partitions: RDD[(Int, String)] = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach ( tuple => {
        sortedMap += tuple.swap
        if (sortedMap.size > topN.value) {
          sortedMap = sortedMap.takeRight(topN.value)
        }
      })
      sortedMap.takeRight(topN.value).toIterator
    })

    val alltopN: Array[(Int, String)] = partitions.collect()
    val finaltopN = SortedMap.empty[Int, String].++:(alltopN)
    val resultUsingMapPartition = finaltopN.takeRight(topN.value)
    //打印结果
    println("-------------- UsingMapPartition计算TopN的结果：--------------")
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t $v")
    }

    // 方法2
    val createCombiner: Int => Int = (v: Int) => v
    val mergeValue: (Int, Int) => Int = (a: Int, b: Int) => a + b
    val moreConciseApproach: Array[(Int, Iterable[String])] =
      kv.combineByKey(createCombiner, mergeValue, mergeValue)
        .map(_.swap)  // 交换Tuple2的两个元素的位置
        .groupByKey()
        .sortByKey(ascending = false)
        .take(topN.value)

    //打印结果
    println("-------------- sortByKey计算TopN的结果：--------------")
    moreConciseApproach.foreach {
      case (k, v) => println(s"$k \t ${v.mkString(",")}")
    }
  }

  /**
    * 3.分组中TopN的算法，就是同一分组Key中取出TopN的数据。
    * @param num TopN 的N值
    * @param filepath 读取文件的路径
    */
  def sparkGroupTopN(num: Int, filepath: String): Unit ={

    val config: SparkConf = new SparkConf().setMaster("local").setAppName("SparkGroupTopN") //Spark配置
    val sparkConext: SparkContext = SparkSession.builder().config(config).getOrCreate().sparkContext //创建上下文

    val linesRdd: RDD[String] = sparkConext.textFile(filepath).filter(line => line.length > 0)  //读取文件数据形成RDD
    val mapredRDD: RDD[(String, Int)] = linesRdd
      .filter(line => line.length > 0) //过滤空行，将rdd转换为键值对PairRDD
      .map(line => line.split(" "))
      .map(arr => (arr(0).trim, arr(1).trim.toInt))
      .cache()  //缓存RDD，方便后期的使用
    val topN: Broadcast[Int] = sparkConext.broadcast(num)

    //1、使用groupByKey的方式实现读取TopN的数据
    //缺点：
    //(1)使用groupByKey，在相同的key所对应的数据形成的迭代器在处理过程中的全部数据会加在到内存中，
    //   如果一个key的数据特别多的情况下，就会很容易出现内存溢出（OOM）
    //(2)在同组key中进行数据聚合并汇总，groupByKey的性能不是很高的，因为没有事先对分区数据进行一个临时聚合运算
    val topNResult1: RDD[(String, Seq[Int])] = mapredRDD.groupByKey().map(tuple2 => {
      val topn = tuple2._2.toList.sorted.takeRight(topN.value).reverse
      (tuple2._1, topn)
    })
    println("-------------- 使用groupByKey获取TopN的结果：--------------")
    println(topNResult1.collect().mkString("\n"))


    //2.使用两阶段聚合，先使用随机数进行分组聚合取局部topN,再聚合取出全局topN的数据
    val topNResult2: RDD[(String, List[Int])] = mapredRDD.mapPartitions(iterator => {
      iterator.map(tuple2 => {
        ((Random.nextInt(10), tuple2._1), tuple2._2)
      })
    }).groupByKey().flatMap({
      //获取values中的前N个值 ，并返回topN的集合数据
      case ((_, key), values) =>
        values.toList.sorted.takeRight(topN.value).map(value => (key, value))
    }).groupByKey().map(tuple2 => {
      val topn = tuple2._2.toList.sorted.takeRight(topN.value).reverse
      (tuple2._1, topn)
    })
    println("-------------- 使用两阶段集合获取TopN的结果：--------------")
    println(topNResult2.collect().mkString("\n"))


    //3、使用aggregateByKey获取topN的记录
    val topNResult3: RDD[(String, List[Int])] = mapredRDD.aggregateByKey(ArrayBuffer[Int]())(
      (u, v) => {
        u += v
        u.sorted.takeRight(topN.value)
      },
      (u1, u2) => {
        //对任意的两个局部聚合值进行聚合操作，可以会发生在combiner阶段和shuffle之后的最终的数据聚合的阶段
        u1 ++= u2
        u1.sorted.takeRight(topN.value)
      }
    ).map(tuple2 => (tuple2._1, tuple2._2.toList.reverse))

    println("-------------- 使用aggregateByKey获取TopN的结果：--------------")
    println(topNResult3.collect().mkString("\n"))
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
    val top5 = ordersDF.orderBy($"total".desc).select("total").take(5)

    import org.apache.spark.sql.functions._
    val top3 = ordersDF.orderBy(desc("total")).select("total").take(3)

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
