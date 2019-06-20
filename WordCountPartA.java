package mavendemo;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;


public class WordCountPartA {
//word count 
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] words = value.toString().trim().split("\\s+");

			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


	//inverse the key value pair
	public static class Job1ToMapper  extends Mapper <Text, Text, IntWritable, Text> {
		private final IntWritable frequency = new IntWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int newVal = Integer.parseInt(value.toString());
			frequency.set(newVal);
			context.write(frequency, key);
		}

	}
//sprt in descending order
	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2,
				int s2, int l2) {
			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			return v1.compareTo(v2) * (-1);
		}
	}
	
	//identity reducer job2 
	public static class SortReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
		Text word = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {

				word.set(value);
				context.write(key, word);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		Path outputpath = new Path(args[1]);
		
		//for codec
	//	conf.setBoolean("mapreduce.compress.map.output", true);
//	    conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);

		
		Job job1 = Job.getInstance(conf, "word count");
		job1.setJarByClass(WordCountPartA.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(outputpath, "job1out1"));
		
		//FileOutputFormat.setCompressOutput(job1, true);
	    //FileOutputFormat.setOutputCompressorClass(job1, SnappyCodec.class);

		job1.setMapperClass(TokenizerMapper.class);
		job1.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(IntSumReducer.class);
		
	//set ouput types for job1
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		Job job2 = Job.getInstance(conf, "sort");
		job2.setJarByClass(WordCountPartA.class);

		FileInputFormat.addInputPath(job2, new Path(outputpath, "job1out1"));
		FileOutputFormat.setOutputPath(job2, new Path(outputpath, "job2out2"));
//set  KeyValueTextInputFormat as input for map job2
		job2.setMapperClass(Job1ToMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
//sortcomprator 
		job2.setSortComparatorClass(IntComparator.class);
		//reducers of task to 1
		job2.setNumReduceTasks(1);
		
		//output of mappers and reducers
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}

		long endTime   = System.currentTimeMillis();
		long totalTime = (endTime - startTime)/60000;
		System.out.println("Performance  in minutes = " + totalTime);
	}


}
