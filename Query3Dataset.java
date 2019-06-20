package demo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class Query3Dataset {
	public static void main(String arr[]) {
	long startTime;
	long executionTime;
	
	startTime = System.currentTimeMillis();
	SparkSession spark = SparkSession.builder()
	         .appName("Q3 version2")
	         .master("local[*]")
	         .getOrCreate();
	  
	     Dataset<Row> df = spark.read().format("xml") 
	         .option("rowTag", "page")  
	        .load("Wiki_data_dump_32GB.xml");
	     Dataset<Row> resultrev1 = df.select("revision");
	     
	     resultrev1.createOrReplaceTempView("revisionView1"); 
	     
	     // revision id, timestamp, contributor id
	     Dataset<Row> filter1 = spark.sql("SELECT revision.id as rid ,to_timestamp(revision.timestamp) AS newdate,revision.contributor.id,revision.contributor.username FROM revisionView1");
	    
	//filter by  multiple contributor id details
	     Dataset<Row> filter2=filter1.groupBy("id").agg(org.apache.spark.sql.functions.count("id").alias("count")).filter("count>1");
	     
	     //join on contributor id 
	     Dataset<Row> joinedDS = filter1.join(filter2, "id");
	     
	     //order by  timestamp
	     Dataset<Row> result= joinedDS.orderBy(org.apache.spark.sql.functions.asc("id"),org.apache.spark.sql.functions.desc("newdate")).drop("count","newdate");
	     
	   executionTime = (System.currentTimeMillis() - startTime)/60000;
	   
	   //write output to external file
	   result.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("outputQ3_v22.csv");
	   
	   System.out.println("Execution time ContributorQ3V2: " + String.valueOf(executionTime));
	  
	   }
}