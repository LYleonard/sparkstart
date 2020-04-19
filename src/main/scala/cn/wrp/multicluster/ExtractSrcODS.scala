package cn.wrp.multicluster

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @Author LYleonard
  * @Date 2020/4/10 20:54
  * @Description 抽取源Hive集群ODS层数据
  */
object ExtractSrcODS {
  def main(args: Array[String]): Unit = {
    var hiveMetaServerAddr: String = ""

    /**
      * 从参数中获取配置，参数形式如下
      * "thrift://master:9083,thrift://slave1:9083"
      */
    hiveMetaServerAddr = args(0)

    val spark: SparkSession = SparkSession.builder().appName("ExtractSrcODS")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("hive.metastore.uris", hiveMetaServerAddr)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "900")
      .config("spark.default.parallelism", "900")
      .config("spark.network.timeout", "300")
      .enableHiveSupport()
      .getOrCreate()

  }

  def changeHDFSConf(spark: SparkSession, nameSpace: String, nn1: String, nn1Addr: String,
                     nn2: String, nn2Addr: String): Unit ={
    val sc: SparkContext = spark.sparkContext
    sc.hadoopConfiguration.set("fd.defaultFS", s"hdfs://$nameSpace")
    sc.hadoopConfiguration.set("dfs.nameservices", nameSpace)
    sc.hadoopConfiguration.set(s"dfs.ha.namenodes.$nameSpace", s"$nn1,$nn2")
    sc.hadoopConfiguration.set(s"dfs.namenodes.rpc-address.$nameSpace.$nn1", nn1Addr)
    sc.hadoopConfiguration.set(s"dfs.namenodes.rpc-address.$nameSpace.$nn2", nn2Addr)
    sc.hadoopConfiguration.set(s"dfs.client.failover.proxy.provider.$nameSpace",
      s"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  }

}
