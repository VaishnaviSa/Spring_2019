package demo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class Query3Sql {
	public static void main(String arr[]) {
	long startTime;
	long executionTime;
	
	startTime = System.currentTimeMillis();
	SparkSession spark = SparkSession.builder()
	         .appName("XML to Dataframe")
	         .master("local[*]")
	         .getOrCreate();
	  
	     Dataset<Row> df = spark.read().format("xml") 
	         .option("rowTag", "page")  
	       .load("Wiki_data_dump_32GB.xml");

	     Dataset<Row> resultrev = df.select("revision");
	     
	     resultrev.createOrReplaceTempView("revisionView"); 
	     
	     // revision id, timestamp, contributor id
	     Dataset<Row> revisionViewDf = spark.sql("SELECT revision.id as rid ,to_timestamp(revision.timestamp) AS newdate,revision.contributor.id FROM revisionView");
	    
	     revisionViewDf.createOrReplaceTempView("df3"); 
	     
	     //filter contributors with count>1
	     Dataset<Row> filtercountdf= spark.sql("SELECT id ,count(*) as count FROM df3 group by id having count(*)>1");
	     
	     filtercountdf.createOrReplaceTempView("filtercountdfView"); 
	     
	     // revision id, timestamp, contributor id
	     Dataset<Row> sqlDFdup = spark.sql("SELECT revision.id as rid ,to_timestamp(revision.timestamp) AS newdate,revision.contributor.id FROM revisionView");
	     
	     sqlDFdup.createOrReplaceTempView("tempdup"); 
	     
	     //join on contributor id 
	    Dataset<Row> joinDF= spark.sql("select tempdup.id as contributorid,tempdup.rid as revisionid,tempdup.newdate as revtime from tempdup join filtercountdfView on filtercountdfView.id=tempdup.id group by tempdup.id,tempdup.rid,tempdup.newdate order by tempdup.newdate desc");
	   
	    joinDF.createOrReplaceTempView("resultView");
	     
	    
	    //group by contributor and order by timestamp
	     Dataset<Row> resultDF=spark.sql("select * from resultView group by contributorid,revisionid,revtime order by contributorid,revtime desc");
	     
	     
	     resultDF.write().format("com.databricks.spark.csv").option("header", "true").save("outputQ4_2.csv");
	     
	     
	     System.out.print("count of rows "+resultDF.count());
	   executionTime = (System.currentTimeMillis() - startTime)/60000;
	   System.out.println("Execution time Q1: " + String.valueOf(executionTime));
	  
	   }
	 

}