package hw4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main1 {
	
	static int windowSize =60;
    static int slideSize=60;
	static String windowDuration = windowSize + " seconds";
    static String slideDuration = slideSize + " seconds";
	public static final int port = 9876;
	public static void main(String[] args) throws InterruptedException, StreamingQueryException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
    	//create a spark session
		SparkSession spark = SparkSession
				  .builder().master("local[4]")
				  .appName("Problem1")
				  .getOrCreate();
		
		//csv schema headers
		StructType schema = DataTypes.createStructType(new StructField[] {
	            DataTypes.createStructField("_c0",  DataTypes.StringType, true),
	            DataTypes.createStructField("_c1", DataTypes.TimestampType, true),
	            DataTypes.createStructField("_c2", DataTypes.StringType, true),
	            DataTypes.createStructField("_c3", DataTypes.StringType, true),
	            DataTypes.createStructField("_c4", DataTypes.StringType, true),
	            DataTypes.createStructField("_c5", DataTypes.StringType, true),
	            DataTypes.createStructField("_c6", DataTypes.StringType, true),
	            DataTypes.createStructField("_c7", DataTypes.StringType, true),
	            DataTypes.createStructField("_c8", DataTypes.StringType, true),
	            DataTypes.createStructField("_c9", DataTypes.StringType, true),
	            DataTypes.createStructField("_c10", DataTypes.StringType, true),
	            DataTypes.createStructField("_c11", DataTypes.StringType, true),
	            DataTypes.createStructField("_c12", DataTypes.StringType, true),
	            DataTypes.createStructField("_c13", DataTypes.StringType, true),
	            DataTypes.createStructField("_c14", DataTypes.StringType, true),
	            DataTypes.createStructField("_c15", DataTypes.StringType, true),
	            DataTypes.createStructField("_c16", DataTypes.StringType, true),
	            DataTypes.createStructField("_c17", DataTypes.StringType, true),
	            DataTypes.createStructField("_c18", DataTypes.StringType, true),
	            DataTypes.createStructField("_c19", DataTypes.StringType, true),
	            DataTypes.createStructField("_c20", DataTypes.StringType, true),
	            DataTypes.createStructField("_c21", DataTypes.StringType, true),
	            DataTypes.createStructField("_c22", DataTypes.StringType, true),
	            DataTypes.createStructField("_c23", DataTypes.StringType, true),
	            DataTypes.createStructField("_c24", DataTypes.StringType, true),
	            DataTypes.createStructField("_c25", DataTypes.StringType, true),
	            DataTypes.createStructField("_c26", DataTypes.StringType, true),
	            DataTypes.createStructField("_c27", DataTypes.StringType, true),
	            DataTypes.createStructField("_c28", DataTypes.StringType, true),
	            DataTypes.createStructField("_c29", DataTypes.StringType, true)
	           
	    });
		
		//readstream from the output file created from server
		Dataset<Row> lines1 =spark.readStream().format("csv").schema(schema).
				load("/Users/vaishnavisabhahith/Desktop/Test/output/*.csv");
		
		lines1.createOrReplaceTempView("res");
		
	//convert the unix time from String to timestamp
		 Dataset<Row> sqlDF1=spark.sql("select  to_timestamp(_c3/1000) as time1,_c15 from res");
		 
		 //calculate the moving average with window and tumbling time
		Dataset<Row> linesavg =sqlDF1.groupBy(functions.window(sqlDF1.col("time1"),"60 seconds","60 seconds")
				).agg(functions.avg(sqlDF1.col("_c15")).alias("MOVINGAVERAGE")).orderBy(org.apache.spark.sql.functions.desc("window"));
			
//calculate the maximum doctets
		Dataset<Row> linesMax =sqlDF1.groupBy(functions.window(sqlDF1.col("time1"),"60 seconds","60 seconds")
				).agg(functions.max(sqlDF1.col("_c15")).alias("MAX")).orderBy(org.apache.spark.sql.functions.desc("window"));
			
		
		linesavg.createOrReplaceTempView("rev3");
		 
		 Dataset<Row> sqlDF3 = spark.sql("SELECT window.start,window.end,MOVINGAVERAGE FROM rev3");
		 
		linesMax.createOrReplaceTempView("revMax");
		 
		 Dataset<Row> sqlDFMax = spark.sql("SELECT window.start,window.end,MAX FROM revMax");
		 
		 //write the output of moving average
		 StreamingQuery query2 = sqlDF3.writeStream()
				 .outputMode("complete")
				    .option("numRows",50)
				   .option("truncate",false)
				   .format("console")
				   .start();
//write the output of maximum doctet
		 StreamingQuery query1 = sqlDFMax.writeStream()
				   .outputMode("complete")
				   .option("numRows",50)
				   .option("truncate",false)
				   .format("console")
				   .start();
		

		 query2.awaitTermination();
		    
		 query1.awaitTermination();
	}

}