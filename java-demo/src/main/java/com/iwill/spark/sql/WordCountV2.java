package com.iwill.spark.sql;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountV2 {

    public static void main(String[] args)  throws Exception{

        SparkSession sparkSession = SparkSession.builder()
                .appName("WordCountV2")
                .config("spark.master","local")
                .getOrCreate();

        Dataset<String> lines = sparkSession.read().textFile("/Users/jiyang12/Github/spark-demo/java-demo/src/main/resources/wordCount.txt");
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        },Encoders.STRING());

        Dataset<Row> wordFrame = words.withColumnRenamed("value","word_name");

        wordFrame.createTempView("tbl_word");
        Dataset<Row> result = sparkSession.sql("select word_name , count(*) from tbl_word group by word_name order by count(*) desc ");
        result.show();
    }
}
