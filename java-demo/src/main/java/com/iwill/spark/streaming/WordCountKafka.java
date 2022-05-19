package com.iwill.spark.streaming;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.Subscribe;
import scala.Tuple2;

import java.util.*;
import java.util.logging.LogManager;

public class WordCountKafka {


    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("WordCountKafka");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf , Duration.apply(1000));

        List<String> topics = Lists.newArrayList("word-count");
        Map<String,Object> kafkaConf = new HashMap<>();
        kafkaConf.put("bootstrap.servers","localhost:9092");
        kafkaConf.put("key.deserializer",StringDeserializer.class);
        kafkaConf.put("value.deserializer", StringDeserializer.class);
        kafkaConf.put("group.id","WordCountKafka");
        kafkaConf.put("enable.auto.commit",false);

        JavaInputDStream<ConsumerRecord<String, String>> inputDStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaConf));
        JavaPairDStream<String,String> results = inputDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {

            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
                return Tuple2.apply(record.key(), record.value());
            }
        });
        JavaDStream<String> lines = results.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2;
            }
        });
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\\s+")).iterator();
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                  return Tuple2.apply(s ,1);
              }
          }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //wordCounts.updateStateByKey();
        wordCounts.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
