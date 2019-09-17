package com.wrp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * * @ClassName WordCountSorted
 * * @Description 降序排列后的wordcount程序
 * * @Author LYleonard
 * * @Date 2019/9/17 15:47
 * Version 1.0
 **/
public class WordCountSorted {
    public static void main(String[] args) {
        wordCountSorted();
    }

    public static void wordCountSorted() {
        // 创建JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("wordCountSorted").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\wordcount.txt");

        // flapMap 切分每行
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // mapToPair 将单词组织成<key, value>的形式
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // reduceByKey 计算单词出现次数
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 要将RDD转换成(3, hello) (2, you)的格式，根据单词出现次数进行排序
        JavaPairRDD<Integer, String> countWords =wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        // 根据单词出现次数排序
        JavaPairRDD<Integer, String> sortedWordCounts = countWords.sortByKey(false);

        // 打印结果 hello: 5
        sortedWordCounts.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._2 + ": " + integerStringTuple2._1);
            }
        });

        sc.close();
    }
}
