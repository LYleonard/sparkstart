package cn.wrp.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * * @Description 累加器：累加器是仅通过关联和交换操作“添加”的变量，因此可以并行有效地支持。
  * * 它们可用于实现计数器（如MapReduce）或总和。Spark本身支持数值类型的累加器，程序员可以添加对新类型的支持。
  * * 作为用户，您可以创建命名或未命名的累加器。如下图所示，命名累加器（在此实例中counter）将显示在Web UI中，
  * * 用于修改该累加器的阶段。Spark显示“任务”表中任务修改的每个累加器的值。
  * * @Author LYleonard
  * * @Date 2019/9/17 14:25
  **/
object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    //errorAccumulatorOps()
    //accumulatorOps()
    accumulatorOps2()
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

  def accumulatorOps2(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("accumulatorOps").setMaster("local")
    val sc: SparkContext= new SparkContext(conf)

    val sum = sc.longAccumulator("accumulator V2")
    val numbers = Array(1, 2, 3, 4, 5)
    val numberRDD = sc.parallelize(numbers)
    numberRDD.foreach(num => sum.add(num))

    println("accumulator sum = " + sum.value)
  }
}
