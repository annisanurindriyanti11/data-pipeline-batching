package com.spark;

import java.io.File;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    SparkSession spark = SparkSession
    .builder()
    .appName("Java Spark Hive Example")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate();    

    spark.sql(" CREATE TABLE IF NOT EXISTS summary AS select stockcode, description, cast(quantity as int)quantity, from_unixtime(unix_timestamp( invoicedate, 'M/d/yyyy HH:mm'))invoicedate, country from ecommerce where from_unixtime(unix_timestamp( invoicedate, 'M/d/yyyy HH:mm'))='2011-07-19 13:01:00' and description='WHITE HANGING HEART T-LIGHT HOLDER' group by stockcode, description, quantity, invoicedate,country order by quantity");
    
    }
}