package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Query1Sql {
    public static void main(String arr[]){
    	long startTime;
    	long executionTime;
    	
    	startTime = System.currentTimeMillis();
    	SparkSession spark = SparkSession.builder()
    	         .appName("XML to Dataframe")
    	         .master("local[*]")
    	         .getOrCreate();
    	  
    	//dataset schema page tag
    	     Dataset<Row> df = spark.read().format("xml") 
    	         .option("rowTag", "page")  
    	        .load("Wiki_data_dump_32GB.xml");
    	     
    	     
    	     //dataset schema revision tag
    	     Dataset<Row> resultrev = df.select("revision");
    	     
    	     
    	     //create a temporary view from resultrev 
    	     resultrev.createOrReplaceTempView("rev");
     	    
    	     //run sql query for struct revision.minor in temporary table rev
    	     Dataset<Row> sqlDF = spark.sql("SELECT count(revision.minor) as MINORCOUNT FROM rev");
    	   
    	     //count the number of rows
    	   sqlDF.show();
    	   
    	   executionTime = (System.currentTimeMillis() - startTime)/60000;
    	   System.out.println("Execution time Q1: " + String.valueOf(executionTime));
    	  
    	   }
    	 

    }
