package com.iwill.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetTest {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("operateTable").config("spark.master", "local").getOrCreate();
        Dataset<Row> dataset  =  sparkSession.read().parquet(getFileList());
        Dataset<Row> result = dataset.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                String adNodeId = value.getAs("campaign_end_date");
                System.out.println(adNodeId);
                //System.out.println("adId : " + adId + ", adGroupId: "+ adGroupId);
                if (adNodeId == null ){
                    return true ;
                }
                return false;
            }
        });
        System.out.println(result.count());
    }

    private static String[] getFileList(){
        String[] result = new String[6];
        for (int i = 0; i <=5 ; i++){
            result[i] =("/Users/jiyang12/Github/spark-demo/src/main/resources/parquet/da-venus/1/data"+(i+1)+".parquet");

        }
        return result;
    }
}
