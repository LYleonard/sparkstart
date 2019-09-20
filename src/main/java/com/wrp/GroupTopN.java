package com.wrp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * * @ClassName GroupTopN
 * * @Author LYleonard
 * * @Date 2019/9/19 15:20
 * * @Description Java实现分组取TopN
 * Version 1.0
 **/
public class GroupTopN {
    public static void main(String[] args) {

        int topN = 3;
        String filepath = "E:\\develop\\Spark\\sparkstart\\src\\test\\scala\\cn\\wrp\\groupTopN.txt";

        TopN(topN, filepath);

    }

    public static void TopN(int topN, String filepath) {
        SparkConf conf = new SparkConf().setAppName("GroupTopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(filepath).filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(String v1) throws Exception {
                if (v1.length() > 0){
                    return true;
                }else {
                    return false;
                }
            }
        });

        JavaPairRDD<String, Integer> linePairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lineSplited = s.split(" ");
                return new Tuple2<String, Integer>(lineSplited[0], Integer.valueOf(lineSplited[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupPairs = linePairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> groupTopN = groupPairs.mapToPair(
            new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Iterable<Integer>>
                call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                    Integer[] topn = new Integer[topN];
                    String key = stringIterableTuple2._1;
                    Iterator<Integer> scores = stringIterableTuple2._2.iterator();
                   while (scores.hasNext()){
                       Integer score = scores.next();
                       for(int i = 0; i < topN; i++) {
                           if(topn[i] == null) {
                               topn[i] = score;
                               break;
                           } else if(score > topn[i]) {
                               for(int j = topN-1; j > i; j--) {
                                   topn[j] = topn[j - 1];
                               }
                               topn[i] = score;
                               break;
                           }
                       }
                   }
                   return new Tuple2<String, Iterable<Integer>>(key, Arrays.asList(topn));
                }
        });

        groupTopN.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1 + ":" + stringIterableTuple2._2);
            }
        });
    }
}
