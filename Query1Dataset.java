package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Query1Dataset {
    public static void main(String arr[]){
    	long startTime;
    	long executionTime;
    	
    	startTime = System.currentTimeMillis();
    	
    	
    	//spark session --tune the executors and partitions 
    	SparkSession spark = SparkSession.builder()
    	         .appName("XML to Dataframe")
    	         .master("local[*]")
    	         //.config("spark.sql.shuffle.partitions", 2001)
    	         .getOrCreate();
    	  
    	//dataset schema page tag
    	     Dataset<Row> df = spark.read().format("xml") 
    	         .option("rowTag", "page")  
    	        .load("Wiki_data_dump_32GB.xml");
    	     
    	     
    	     //filter the minor tag which is not eqaul to null
    	     Dataset<Row> df2 =df.filter(df.col("revision.minor").isNotNull());
    	
    	     //count the number of rows
    	     System.out.print("count of rows "+df2.count());
    	     
    	   executionTime = (System.currentTimeMillis() - startTime)/60000;
    	   System.out.println("Execution time Q1: " + String.valueOf(executionTime));
    	  
    	   }
    	 

    }
