package cn.wrp

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * 通过JDBC连接Mysql数据库，查询表，获取DataFrame.需要在pom.xml中添加mysql JDBC驱动
  * <dependency>
  * <groupId>mysql</groupId>
  * <artifactId>mysql-connector-java</artifactId>
  * <version>5.1.47</version>
  * </dependency>
  */
object ConMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MysqlQueryDemo")
      .master("local").getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/band?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "accounts")
      .option("user", "root")
      .option("password", "243015")
      .load()
    jdbcDF.show()

    UpdateMysql(jdbcDF)
  }

  def UpdateMysql(dataset: DataFrame): Unit = {

    val sparkSession = SparkSession.builder().appName("insertTable")
      .master("local").getOrCreate()

//    val dataset = sparkSession.read.option("header", "true")
//      .csv("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\accounts.csv")

    dataset.show()

    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "243015")

    dataset.write.mode(SaveMode.Append).jdbc(url=url, "accounts_copy", prop)

  }
}
