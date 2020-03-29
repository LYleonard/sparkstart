package cn.wrp.Utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

    /**
      * 以下方法在spark2.2.3版本后可使用，2.0.1版本不可用
      */
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

  /**
    * 使用RDD处理多分隔符，并使用Row类型RDD转换成DataFrame类型
    * @param inputpath
    * @param mutidelimiter
    * @return
    */
  def readByMultiDelimiter2(inputpath: String, mutidelimiter:String, schemaString:String):DataFrame={
    val rdd = spark.sparkContext.textFile(inputpath)
    // 2.定义schema，带有StructType的
    // 对schema信息按空格进行分割,最终fileds里包含了4个StructField
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 3.把schema信息作用到RDD上,这个RDD里包含了一些行,形成Row类型的RDD
    val rowRDD = rdd.filter(_!="").map(_.split(mutidelimiter))
      .filter(line => line(0)!=fields(0).name && line.length==fields.length)//过滤列宽对不齐的数据
      .map(x => Row.fromSeq(x.toSeq))

    // 通过SparkSession创建DataFrame,传入rowRDD和schema
    val DF = spark.createDataFrame(rowRDD, schema)
    DF
  }

  /**
    * wholeTextFiles读入多个文件，并处理多分隔符，使用
    * @param inputpath
    * @param mutidelimiter
    * @param schemaString
    * @return
    */
  def readByMultiDelimiter3(inputpath: String, mutidelimiter:String, schemaString:String):DataFrame={
    // 通过SparkSession创建DataFrame,传入rowRDD和schema
    // 对schema信息按空格进行分割,最终fileds里包含多个StructField
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val columnName:String = fields(0).name
    val fieldsLength = fields.length

    // 多个文件同时读入，wholeTextFiles读取小文件比TextFile更高效
    val rdd = spark.sparkContext.wholeTextFiles(inputpath)

    /*//测试
    val a = rdd.map(record=>record._2).map(lines=>lines.split("\r\n")).flatMap(lines=>lines).filter(_!= "")
    val b = rdd.map(record=>record._2).map(lines=>lines.split("\r\n")).flatMap(lines=>lines).filter(_!= "")
      .map(x => x.split(mutidelimiter))
    val c = rdd.map(record=>record._2).map(lines=>lines.split("\r\n")).flatMap(lines=>lines).filter(_!= "")
      .map(x => x.split(mutidelimiter)).filter(_.length==fieldsLength)
      .map(lines => {print(lines.toSeq);Row.fromSeq(lines.toSeq)})
    val DF1 = spark.createDataFrame(c,schema)
    DF1.show(12)*/

    val rowRDD = rdd.map(record=>record._2)
      .map(lines=>lines.split("\r\n"))
      .flatMap(lines=>lines)
      .filter(_!= "")//去空行
      .map(x => x.split(mutidelimiter))
      .filter(_.length==fieldsLength)//过滤列宽对不齐的数据
      .map(fields => Row.fromSeq(fields.toSeq))

    val DF = spark.createDataFrame(rowRDD, schema)
      .filter(col(columnName).notEqual(columnName))//如果文件包含header，进行该步过滤操作
    DF
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

    val inputPath2 = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\multidelimiter.csv"
    val inputPath3 = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\multidelimiterfiles"

    val multiDelimiterPattern = "\\|\\|"
    val schemaString:String = "id,name,score,course"

    val multiDelimiterParse = new MultiDelimiterParse(spark)
    val df = multiDelimiterParse.readByMultiDelimiter2(inputpath = inputPath2, multiDelimiterPattern, schemaString)
    df.show(24)

    val df2 = multiDelimiterParse.readByMultiDelimiter3(inputPath3, multiDelimiterPattern, schemaString)
    df2.show(24)
  }
}
