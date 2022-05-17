package com.iwill.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class DailyUVAnalyse {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DailyUVAnalyse")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<String> uvDs = sparkSession.read().textFile("/Users/jiyang12/Github/spark-demo/java-demo/src/main/resources/uv.txt");
        Encoder encoder = Encoders.bean(Row.class);
        Dataset<Row> originRow = uvDs.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                String[] arr = value.split(",");
                return RowFactory.create(arr[0], arr[1]);
            }
        }, encoder);

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("date", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("userId", DataTypes.StringType,false, Metadata.empty()),
                }
        );
        Dataset<Row> dataframe = sparkSession.createDataFrame(originRow.toJavaRDD() ,schema);
        dataframe.groupBy(col("date"))
                .agg(countDistinct("userId").alias("userIdCount"))
                .show();
    }
}
