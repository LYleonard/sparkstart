package cn.wrp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description 累加器：Accumulator是spark提供的累加器，该变量只能够增加。
  * * 只有driver能获取到Accumulator的值（使用value方法），Task只能对其做增加操作（使用 +=）。
  * * 你也可以为Accumulator命名，这样就会在spark web ui中显示，可以帮助你了解程序运行的情况。
  * * @Author LYleonard
  * * @Date 2019/9/17 14:25
  **/
object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    //errorAccumulatorOps()
    accumulatorOps()
  }

  def errorAccumulatorOps(): Unit = {
    /** 这是一个错误的使用 !!!
      * 对于仅在操作内执行的累加器更新，Spark保证每个任务对累加器的更新仅应用一次，
      * 即重新启动的任务不会更新该值。 在转换中，用户应该知道，如果重新执行任务或
      * 作业阶段，则可以多次应用每个任务的更新。
      */
    val conf = new SparkConf().setAppName("accumulatorOps").setMaster("local")
    val sc = new SparkContext(conf)

    val accum= sc.accumulator(0, "Error Accumulator")
    val data = sc.parallelize(1 to 10)

    //用accumulator统计偶数出现的次数，同时偶数返回0，奇数返回1
    val newData = data.map{x => {
      if(x%2 == 0){
        accum += 1
        accum
      }
    }}

    //使用action操作触发执行
    newData.count

    //此时accum的值为5，是正确的结果
    println("First print: " + accum.value)

    //继续操作，在执行一次action操作
    newData.count()

    //上个步骤没有进行累计器操作，可是累加器此时的结果已经是10了
    //这是错误的结果
    println("Second print: " + accum.value)
  }

  def accumulatorOps(): Unit ={
    /**
     ** 使用Accumulator时，为了保证准确性，只使用一次action操作。
     ** 如果需要使用多次则使用cache或persist操作切断依赖。
     */
    val conf = new SparkConf().setAppName("accumulatorOps").setMaster("local")
    val sc = new SparkContext(conf)

    val accum= sc.accumulator(0, "Error Accumulator")
    val data = sc.parallelize(1 to 10)

    val newData = data.map{x => {
      if(x%2 == 0){
        accum += 1
        accum
      }
    }}

    //使用cache缓存数据，切断依赖。
    newData.cache.count

    //此时accum的值为5
    println("First print: " + accum.value)
    newData.count()

    //此时的accum依旧是5
    println("Second print: " + accum.value)
  }
}