package com.wrp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * * @ClassName SecondarySort
 * * @Author LYleonard
 * * @Date 2019/9/18 0:39
 * * @Description 二次排序
 *  * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 *  * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 *  * 3、使用sortByKey算子按照自定义的key进行排序
 *  * 4、再次映射，剔除自定义的key，只保留文本行
 * Version 1.0
 **/
public class SecondarySort {
    public static void main(String[] args) {
        // 创建 SparkConf, JavaSparkContext
        SparkConf conf = new SparkConf()
                .setAppName("SecondarySort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\sort.txt");

        // 映射为(key, line) Tuple2, key由SecondarySortKey生成
        JavaPairRDD<SecondarySortKey, String> pair = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public static final long serialVersionUID = 1L;
            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineCol = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.valueOf(lineCol[0]),Integer.valueOf(lineCol[1]));
                return new Tuple2<SecondarySortKey, String>(key, line);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortPairs = pair.sortByKey();

        JavaRDD<String> sortedLines = sortPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        sortedLines.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
