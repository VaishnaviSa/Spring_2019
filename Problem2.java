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

public class Problem2 {
	static int windowSize =60;
    static int slideSize=60;
	static String windowDuration = windowSize + " seconds";
    static String slideDuration = slideSize + " seconds";
	public static final int port = 9876;
	public static void main(String[] args) throws InterruptedException, StreamingQueryException {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
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
		
		SparkSession spark = SparkSession
				  .builder().master("local[*]")
				  .appName("Problem2")
				  .getOrCreate();
	
		Dataset<Row> lines1 =spark.readStream().format("csv").schema(schema).
				load("/Users/vaishnavisabhahith/Desktop/Test/output/*.csv");
		
		lines1.createOrReplaceTempView("res");
		
		Dataset<Row> sqlDF1=spark.sql("select  to_timestamp(_c3/1000) as time1,_c9,_c10 from res"); 
		//tunblign and sliding window
		Dataset<Row> right =sqlDF1.withWatermark("time1", "10 minutes").groupBy(functions.window(sqlDF1.col("time1"),"120 seconds","60 seconds")
				,sqlDF1.col("_c9"),sqlDF1.col("_c10")).count();
	
		right.createOrReplaceTempView("res1");
		//filter the count>1
		Dataset<Row> right1 =spark.sql("select  window.start,window.end,_c9,_c10 from res1 where count>1");
		
		//ouput the source and destination IP
		StreamingQuery query1 = right1.writeStream()
				   .outputMode("complete").option("truncate", false)
				   .format("console")
				   .start();
		 
		 query1.awaitTermination();	
	}

}