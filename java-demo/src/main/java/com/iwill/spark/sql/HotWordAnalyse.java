package com.iwill.spark.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * ref : https://www.codeleading.com/article/8136494279/
 */
public class HotWordAnalyse {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("HotWordAnalyse")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<String> uvDs = sparkSession.read().textFile("/Users/jiyang12/Github/spark-demo/java-demo/src/main/resources/hotWord.txt");
        Encoder encoder = Encoders.bean(Row.class);
        Dataset<Row> originRow = uvDs.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                String[] arr = value.split(",");
                return RowFactory.create(arr[0], arr[1], arr[2]);
            }
        }, encoder);
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("date", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("username", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("keyword", DataTypes.StringType,false, Metadata.empty())
                }
        );
        Dataset<Row> dataframe = sparkSession.createDataFrame(originRow.toJavaRDD() ,schema);
        Dataset<Row> df = dataframe.select(new Column("date") ,new Column("keyword") ,new Column("username"))
                .groupBy("date","keyword")
                .agg(countDistinct("username").alias("uc"))
                .orderBy(new Column("date").asc() ,new Column("uc").desc());

        df.select(new Column("date") ,new Column("keyword"),new Column("uc"),
                row_number().over(Window.partitionBy(new Column("date"))
                .orderBy(new Column("uc").desc())).alias("rank"))
                .where("rank<=3")
                .show();

    }
}
