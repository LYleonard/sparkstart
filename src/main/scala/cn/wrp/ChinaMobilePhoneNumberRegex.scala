package cn.wrp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Author LYleonard
  * @Date 2019/11/15 16:07
  * @Description scala正则表达式过滤中国手机号
  */
object ChinaMobilePhoneNumberRegex {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().
      appName("ChinaMobilePhoneNumberFilter").master("local[*]").getOrCreate()
    // 日志级别设置测试
//    sparkSession.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

    val phonesDF = sparkSession.read.option("header", "true").csv("E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\phones.csv")
    val purePhonesDF = phonesDF.filter(x=>{
      println("X: ", x)
      var flag=false
      val phoneNum=x.getAs[String]("phones")
      val pattern = """^(?:\+?86)?1(?:3\d{3}|5[^4\D]\d{2}|8\d{3}|7(?:[01356789]\d{2}|4(?:0\d|1[0-2]|9\d))|9[189]\d{2}|6[567]\d{2}|4(?:[14]0\d{3}|[68]\d{4}|[579]\d{2}))\d{6}$""".r

      println(pattern findFirstMatchIn phoneNum)
      println("----------------------")

      if ((pattern findFirstMatchIn phoneNum) != None){ flag = true }
      flag
    })

    purePhonesDF.show()
  }
}
