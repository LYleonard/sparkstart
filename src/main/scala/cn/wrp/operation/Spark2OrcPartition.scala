package cn.wrp.operation

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author LYleonard
  * @Date 2020/3/31 23:52
  * @Description TODO
  */
object Spark2OrcPartition {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("TestPhoneGeo")
//      .master("local")   //
      .enableHiveSupport()            // spark读写hive表配置
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    val DF = sparkSession.read.option("header","true").csv("hdfs://192.168.29.129:9000/test/phones.csv")

    // test.phone_orc表以level列字段分区
    DF.withColumn("level", lit(1)).write.mode(SaveMode.Append).format("orc").insertInto("test.phone_orc")
  }
}
