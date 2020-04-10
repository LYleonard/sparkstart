package cn.wrp.operation

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * @Author LYleonard
  * @Date 2019/11/26 17:07
  * @Description 操作HDFS上文件/文件夹
  */
object HdfsOperation {
  def main(args: Array[String]): Unit = {
    """直接引入hadoop相关包"""
    val config = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://master:8020"), config)
    val path =  new Path("/tmp/install_java.sh")
    if (fs.exists(path)){
      println("This file is not null")
    }

    """使用Spark SQL SparkSession.sparkContext.hadoopConfiguration"""
    val spark = SparkSession.builder()
      .appName("HdfsOperation")
      .master("local")
      .getOrCreate()
    val sc =spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs1 = FileSystem.get(conf)
    val exists = fs1.exists(new Path("/tmp/install_java.sh"))

    """操作hdfs文件"""
    //创建文件夹
    //    val flag = fs.mkdirs(new Path("/test"))
    //    println(flag)

    //自动创建文件info.txt 并写数据
    //    val out = fs.create(new Path("/test/info.txt"))
    //    out.write("HDFS写数据".getBytes())
    //    out.flush()
    //    out.close()
    //    fs.close()

    //重命名文件
    //    val src = new Path("/test/info.txt")
    //    val dst = new Path("/test/info_1.txt")
    //    val flag = fs.rename(src,dst)
    //    println(flag)

    //删除文件 1
    //    val path =  new Path("/test/info_1.txt")
    //
    //    if (fs.exists(path)){
    //      val flag = fs.delete(path)
    //      println(flag)
    //    }


    //删除文件夹 1
//    val path1 = new Path("/test/")
//
//    val flag = fs.deleteOnExit(path1)
//    println(flag)
  }
}
