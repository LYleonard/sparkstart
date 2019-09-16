package com.wrp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * * @ClassName TransformationOps
 * * @Description Spark transformation operations example by Java
 * * @Author LYleonard
 * * @Date 2019/9/16 16:17
 * Version 1.0
 **/
public class TransformationOps {
    public static void main(String[] args) {
        //mapOperation();
        //groupByKeyOps();
        //joinOps();
        cogroupOps();
    }

    public static void mapOperation(){
        // 创建SparkConf
        SparkConf conf = new SparkConf().setAppName("mapOperation").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);
        // 并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        // 使用map算子将集合中的每个元素都乘以2
        // 在java中，map算子接收的参数是function对象
        JavaRDD<Integer> mulitipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        // foreach打印
        mulitipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void groupByKeyOps() {
        SparkConf conf = new SparkConf().setAppName("groupByKeyOps").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 85),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 98),
                new Tuple2<String, Integer>("class2", 88)
        );

        JavaPairRDD<String, Integer>  scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> groupScores = scores.groupByKey();

        groupScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println("Class Name: " + stringIterableTuple2._1);
                Iterator<Integer> iterable = stringIterableTuple2._2.iterator();

                while (iterable.hasNext()){
                    System.out.println(iterable.next());
                }
            }
        });
    }

    public static void joinOps(){
        SparkConf conf = new SparkConf().setAppName("joinAndCogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentsList = Arrays.asList(
                new Tuple2<Integer, String>(1, "lee"),
                new Tuple2<Integer, String>(2, "Jack"),
                new Tuple2<Integer, String>(3, "Tom")
        );
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 98),
                new Tuple2<Integer, Integer>(2, 75),
                new Tuple2<Integer, Integer>(3, 87)
        );

        JavaPairRDD<Integer, String> studentsRDD = sc.parallelizePairs(studentsList);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, Tuple2<String, Integer>> studentsScores = (JavaPairRDD<Integer, Tuple2<String, Integer>>) studentsRDD.join(scoresRDD);

        studentsScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1);
                System.out.println(integerTuple2Tuple2._2._2);
                System.out.println("================================");
            }
        });
    }

    public static void cogroupOps(){
        SparkConf conf = new SparkConf().setAppName("joinAndCogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentsList = Arrays.asList(
                new Tuple2<Integer, String>(1, "lee"),
                new Tuple2<Integer, String>(2, "Jack"),
                new Tuple2<Integer, String>(3, "Tom")
        );
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 98),
                new Tuple2<Integer, Integer>(2, 75),
                new Tuple2<Integer, Integer>(3, 87),
                new Tuple2<Integer, Integer>(1, 97),
                new Tuple2<Integer, Integer>(2, 85),
                new Tuple2<Integer, Integer>(3, 77)
        );

        JavaPairRDD<Integer, String> studentsRDD = sc.parallelizePairs(studentsList);
        JavaPairRDD<Integer, Integer> scoresRDD = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentsScores =
                (JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>) studentsRDD.cogroup(scoresRDD);

        studentsScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._1);
                System.out.println(integerTuple2Tuple2._2._1);
                System.out.println(integerTuple2Tuple2._2._2);
                System.out.println("================================");
            }
        });
    }
}
