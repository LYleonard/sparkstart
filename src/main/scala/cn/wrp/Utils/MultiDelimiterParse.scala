package cn.wrp.Utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @Author LYleonard
  * @Date 2020/1/4 21:20
  * @Description Spark SQL 处理多分割符情况
  */
class MultiDelimiterParse(@transient spark: SparkSession){
  import spark.implicits._

  /**
    * 由于Spark SQL去取csv时只能指定单个符号的分割符，
    * 该方法实现用于处理多分隔符（multiChar）如"||"等分割符。
    * @param inputpath
    * @param mutidelimiter
    * @return DataFrame类型
    */
  def readByMultDelimiter(inputpath: String, mutidelimiter:String):DataFrame={
    //    val df = spark.read.textFile(inputpath).map(line => {
    ////      line.split(mutidelimiter).mkString(",")
    //      line.replaceAll(mutidelimiter, ",")
    //    })
    //    val DF = spark.read.option("header", "true").option("delimeter", ",").csv(df)
    //    DF

    val df = spark.read.option("header", "true").csv(spark.read.textFile(inputpath)
      .map(line => {
        //        line.split(mutidelimiter).mkString(",")
        line.replaceAll(mutidelimiter, ",")
      }))
    df
  }

  def readByMultiDelimiter1(inputpath: String, mutidelimiter:String):DataFrame={
    val df = spark.read.option("header", "false").csv(inputpath)
    // 获取长度
    val COLEN:Int = df.take(1)(0)(0).toString.split(mutidelimiter).length

    var splitedDF = df.withColumn("_c00", split(col("_c0"), mutidelimiter)
      .getItem(0))
    if (COLEN > 1){
      for (i <- 1 until COLEN-1){
        splitedDF = splitedDF.withColumn("_c0"+i, split(col("_c0"), mutidelimiter).getItem(i))
      }
    }
    splitedDF.drop("_c0")
  }
}

object MultiDelimiterParse{
  /**
    * 主函数用于测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MultiDelimiterParse")
      .master("local") // 集群运行时，注释掉
      .getOrCreate()

    val inputPath = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\multidelimiter.csv"
    val multiDelimiterPattern = "\\|\\|"

    val multiDelimiterParse = new MultiDelimiterParse(spark)
    val df = multiDelimiterParse.readByMultDelimiter(inputpath = inputPath, mutidelimiter = multiDelimiterPattern)
    df.show()
  }
}
