package com.iwill.spark.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) {

        //String input = "/Users/jiyang12/Github/spark-demo/src/main/resources/wordOperate.txt";
        //String output = "/Users/jiyang12/Github/spark-demo/src/main/resources/result.txt";
        String input = args[0];
        String output = args[1];
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount");
        JavaSparkContext context  = new JavaSparkContext(sparkConf);
        JavaRDD rdd = context.textFile(input ,2);
        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction() {
            @Override
            public Iterator call(Object o) throws Exception {
                return Arrays.asList(String.valueOf(o).split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Long> pairs = words.mapToPair(s -> new Tuple2<>(s, 1L));
        JavaPairRDD<String, Long> counts =  pairs.reduceByKey((Function2<Long, Long, Long>) Long::sum);

        System.out.println("Starting task..");
        long t = System.currentTimeMillis();
        counts.saveAsTextFile(output + "_" + t);
        System.out.println("Time=" + (System.currentTimeMillis() - t));
        context.stop();
    }
}
