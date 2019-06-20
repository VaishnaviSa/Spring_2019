package mavendemo;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.WritableComparator;
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


public class WordCountPartB {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] words = value.toString().trim().split("\\s+");

			//filter for more than 3 characters
			for (String word : words) {
				if(word.length()>3)
				context.write(new Text(word), new IntWritable(1));
			}

		}
	}
//job1 reducer
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

		Job job1 = Job.getInstance(conf, "word count");
		job1.setJarByClass(WordCountPartB.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(outputpath, "job1out1"));
	    
	    job1.setMapperClass(TokenizerMapper.class);
	    job1.setCombinerClass(IntSumReducer.class);
	    job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
	    
        if (!job1.waitForCompletion(true)) {
      	  System.exit(1);
      	  }
      
      Job job2 = Job.getInstance(conf, "sort");
      job2.setJarByClass(WordCountPartB.class);
      
      FileInputFormat.addInputPath(job2, new Path(outputpath, "job1out1"));
      FileOutputFormat.setOutputPath(job2, new Path(outputpath, "job2out2"));
	    
      job2.setMapperClass(Job1ToMapper.class);
      job2.setReducerClass(SortReducer.class);
      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setSortComparatorClass(IntComparator.class);
      job2.setNumReduceTasks(1);
      job2.setMapOutputKeyClass(IntWritable.class);
      job2.setMapOutputValueClass(Text.class);
      
     
      if (!job2.waitForCompletion(true)) {
      	  System.exit(1);
      	}

		long endTime   = System.currentTimeMillis();
		long totalTime = (endTime - startTime)/60000;
		System.out.println("Performance  in minutes  = " + totalTime);
	}


}
