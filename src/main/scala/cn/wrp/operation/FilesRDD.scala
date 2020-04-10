package cn.wrp.operation

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * * @Description 利用文件创建RDD，并写入Mysql
  * * @Author LYleonard
  * * @Date 2019/9/16 11:07
  **/
object FilesRDD {

  // 创建SparkSession对象
  val sparkSession = SparkSession.builder().appName("FilesRDD")
    .master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    filesRDD()
  }

  // 创建student类
  case class Student(id:Int, Fname:String, age:Int)
  def filesRDD():Unit = {
    /**
      * 利用集合 List 创建 RDD
      */
    // 读入文件数据
    val studentfileRDD: RDD[String] = sparkSession.sparkContext
      .textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\students.txt")
    // 行内切分数据
    val arrRDD: RDD[Array[String]] = studentfileRDD.map(_.split(","))

    // RDD关联Student类
    val studentRDD: RDD[Student] = arrRDD.map(x => Student(x(0).toInt, x(1), x(2).toInt))

    // 导入隐式转换
    import sparkSession.implicits._
    val studentDF = studentRDD.toDF() // RDD转换为DataFrame
    studentDF.createOrReplaceTempView("student")  //创建临时视图
    val result = sparkSession.sql("SELECT * FROM student ORDER BY age DESC") //查询临时视图并按age排序

    // 结果写入Mysql
    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8"
    val property = new Properties()
    property.put("user", "root")
    property.put("password", "243015")
    result.write.mode(SaveMode.Append).jdbc(url, "student", property)
  }
}
