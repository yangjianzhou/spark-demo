package com.iwill.spark.sql;

import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.List;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class SparkTest {

    public static void main(String[] args) throws Exception {
        //wordOperate();
        operateTable();
        //transferData();
        //basicOperation();
    }

    private static void basicOperation() throws Exception{
        SparkSession sparkSession = SparkSession.builder().appName("basicOperation").config("spark.master", "local").getOrCreate();
        Dataset<String> dataset = sparkSession.read().textFile("/Users/jiyang12/Github/spark-demo/java-demo/src/main/resources/person.txt");
        Encoder<Person> encoder = Encoders.bean(Person.class);
        Dataset<Person> personDataSet = dataset.map(new MapFunction<String, Person>() {
            @Override
            public Person call(String value) throws Exception {
                String[] list = value.split(",");
                return new Person(Integer.parseInt(list[0]),list[1], Integer.parseInt(list[2]));
            }
        }, encoder);
        Dataset<Row>  personDataFrame = personDataSet.toDF();
        personDataFrame.show();
        personDataFrame.createTempView("tbl_person");
        Dataset<Row>  selectedPersonDataFrame  = sparkSession.sql("SELECT * FROM tbl_person WHERE id = 1");
        selectedPersonDataFrame.show();

    }

    private static void wordOperate() {
        SparkSession sparkSession = SparkSession.builder().appName("wordOperate").config("spark.master", "local").getOrCreate();
        JavaRDD<String> lines = sparkSession.read().textFile("/Users/jiyang12/Github/spark-demo/src/main/resources/wordOperate.txt").javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Splitter.on(",").omitEmptyStrings().split(s).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple2 : output) {
            System.out.println(tuple2._1() + " : " + tuple2._2());
        }
        sparkSession.stop();
    }

    private static void operateTable() throws Exception {
        SparkSession sparkSession = SparkSession.builder().appName("operateTable").config("spark.master", "local").getOrCreate();
        sparkSession.udf().register("test_udf", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                if (s.equals("shanghai")){
                    return "correct address";
                }
                return "wrong address";
            }
        }, DataTypes.StringType);

        Dataset<Row> df = sparkSession.read()
                .format("jdbc")
                .option("user","root")
                .option("password","12345678")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/test")
                .option("dbtable","(select * from test_1 where age<=2) as t")
                //.option("query","select * from test_1 where age<=2")
                .load();
        df.show();
        df.createTempView("test_1");
        Dataset<Row> test = sparkSession.sql("select test_udf(address) from test_1");
        test.show();

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "12345678");
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> rows = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/xxl_job", "xxl_job_info", properties);

        rows.foreach(row -> {
            System.out.print("==========================");
            System.out.print("id : " + row.getInt(0));
            System.out.print(" ,executor_address : " + row.getString(2));
            System.out.print(",trigger_time : " + row.getTimestamp(3));
            System.out.println("");
        });
        sparkSession.stop();
    }

    private static void transferData() {
        SparkSession sparkSession = SparkSession.builder().appName("transferData").config("spark.master", "local").getOrCreate();
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "12345678");
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> personInfoDs = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test", "test_1", properties).select("*");
        //Dataset<Row> deequ_test_1 = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test", "deequ_test_1", properties).select("*");
        //Dataset<Row> jointResult = deequ_test.join(deequ_test_1 ,deequ_test_1.col("id").equalTo(deequ_test.col("id")) ,"left");
        //deequ_test = deequ_test.withColumn("map",functions.map(deequ_test.col("id"),deequ_test.col("settlement_type"),deequ_test.col("age"),deequ_test.col("ad_engine")));
        //jointResult.show();
        //System.out.println(sparkSession.sql("select * from test.deequ_test"));
   /*     deequ_test = deequ_test.select("ad_engine", "age").groupBy("ad_engine").agg(functions.sort_array(functions.collect_list("age")).alias("ages"));
        deequ_test = deequ_test.withColumn("version", functions.lit(null).cast(DataTypes.IntegerType));
        deequ_test = deequ_test.withColumn("ad_engines", functions.col("ad_engine"));
        deequ_test.show();*/
       /* test1 = test1.select(col("original_keywords").cast(TimestampType).as("campaign_end_datetime"));
        Dataset<Row> test2 = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test", "test_2", properties).select("*");
        Dataset<Row> test3 = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/test", "test_3", properties).select("*");
*/
        personInfoDs = personInfoDs.withColumn("test_date",lit("29991231"));
        personInfoDs = personInfoDs.withColumn("base_date_1",lit("29991231"));
        personInfoDs = personInfoDs.select(col("base_date_1"), col("test_date").cast(TimestampType).as("test_1"));
        personInfoDs = personInfoDs.withColumn("test_date",lit("2999-12-31 00:00:00.000"));
        personInfoDs = personInfoDs.withColumn("base_date_2",lit("2999-12-31 00:00:00.000"));
        personInfoDs = personInfoDs.select(col("base_date_1"),col("test_1"),col("base_date_2"),col("test_date").cast(TimestampType).as("test_2"));
        personInfoDs.show();
/*        personInfoDs = personInfoDs.drop("age");
        List<String> schemaColumns = asList(StructType.fromDDL("person_info.ddl").fieldNames());
        personInfoDs =personInfoDs.select(schemaColumns.get(0),schemaColumns.subList(1 ,schemaColumns.size()).toArray(new String[0]));
        personInfoDs.write().mode(Overwrite).*//*format()*//*option("compression","snappy").save("test/person_info");*/
/*        test1.write()
                .format("jdbc")
                .mode(Append)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/test")
                .option("user", "root")
                .option("dbtable", "test_backup_1")
                .option("password", "12345678")
                .save();*/
      /*
        Dataset<Row> venusCampaignAdzerkNormalized =
                test1
                        .join(
                                test2,
                                test1
                                        .col("name")
                                        .equalTo(test2.col("name_2")),
                                "left")
                        .join(
                                test3,
                                test1
                                        .col("type")
                                        .equalTo(test3.col("type_3")),
                                "left");
        venusCampaignAdzerkNormalized.show();
        sparkSession.stop();*/
    }

    public static void saveDatasetIntoHdfs(){
        System.setProperty("HADOOP_USER_NAME","root");
        SparkSession sparkSession = SparkSession.builder().appName("transferData").config("spark.master", "local").getOrCreate();
        SparkConf sc = new SparkConf();
        sc.setAppName("saveDataset");
        sc.setMaster("yarn");
        sc.set("yarn.resourcemanager.hostname","master");
        sc.set("spark.executor.instance","2");
        sc.set("spark.executor.memory","1024M");
        sc.set("spark.yarn.queue","spark");
        sc.set("spark.driver.host","192.168.0.105");
        sc.setJars(new String[]{""});
        sc.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
    }


}
