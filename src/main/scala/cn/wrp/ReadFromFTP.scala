package cn.wrp

import org.apache.spark.sql.SparkSession

/**
  * @Description 读写ftp服务器
  * @Author LYleonard
  * @Date 2020-1-2 16:33
  *       Version 1.0
  **/
object ReadFromFTP {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ReadDataFromFTP").getOrCreate()

    val df = sparkSession.read.
      format("com.springml.spark.sftp").
      option("host", "cp-nifi-cluster-01-lkj").//主机名
      option("username", "root").//注意这个的用户名为系统用户名或者具有指定路径访问权限的用户名
      option("port", "22").
      option("header", "false").
      option("password", "cetc@2017").
      option("fileType", "csv").
      option("delimiter", ",").
//      option("quote", "\"").
      option("createDF", "true").
//      option("escape", "\\").
      option("multiLine", "true").
      option("inferSchema", "true").
      load("/home/ftpuser/pub/testftp/") //系统目录，不是ftp的访问的根目录

    df.show()

  }
}
