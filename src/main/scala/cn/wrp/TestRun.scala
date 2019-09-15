package cn.wrp

import com.wrp.PhoneNumberGeo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestRun {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("wordcount")
//      .getOrCreate()
//    import spark.implicits._
//    val df = spark.read.json("E:\\develop\\TestSpark\\src\\test\\scala\\samples\\iris.json")
//
//    df.show()
//    df.printSchema()
    phoneGeo()
  }

  //  def main(args: Array[String]): Unit = {
  //
  //    val conf = new SparkConf()
  //    conf.setAppName("WordCount").setMaster("local")
  //    val hive = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
  //
  //    val df = hive.sql("select * from iris.iris limit 10")
  //    df.collect().foreach(println)
  //
  //  }
  def phoneGeo(): Unit ={
    /**
      * 测试归属地接口
      */
    val sparkSession = SparkSession.builder().appName("TestPhoneGeo")
      .master("local")   //
      .enableHiveSupport()            // spark读写hive表配置
      .config("mapreduce.input.fileinputformat.input.dir.recursive", true)
      .config("hive.input.dir.recursive", true)
      .config("hive.mapred.supports.subdirectories", true)
      .config("hive.supports.subdirectories", true)
      .config("spark.sql.antoBroadcastJoinThreadhold", 1024*1024*100)  //小于100M的文件Join时自动开启Broadcast Join
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.consolidateFiles", true)  //
      .config("spark.speculation", true)
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.udf.register("GetNumLocation", (phonenumber:String) =>{
      val phoneNumberGeo = new PhoneNumberGeo()
      val numgeo = phoneNumberGeo.lookup(phonenumber)

      var geo: String = "others"
      if (numgeo != null && "上海".equals(numgeo.getProvince)){
        geo = "Shanghai"
      }
      geo
    })

    val phoneGeoInfo = sparkSession.read.csv("hdfs://master:8020/test/phones.csv")
      .withColumn("geo", callUDF("GetNumLocation", $"phones"))

    val phoneGeoInfo1 = sparkSession.table("default.phones").filter($"insert_time" === "2019-09-12")

    phoneGeoInfo.show()

  }
}
