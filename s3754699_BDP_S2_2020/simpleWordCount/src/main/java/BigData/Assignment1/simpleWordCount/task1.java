package BigData.Assignment1.simpleWordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



// Write a MapReduce program to count number of short words (1-4 letters),
// medium words (5-7 letters) words, long words (8-10 letters) and 
// extra-long words (More than 10 letters).

public class task1 {
	
	public static final Logger LOG = Logger.getLogger(task1.class);
	
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
				
		private final static IntWritable one = new IntWritable(1);
		private Text short_word = new Text();
		private Text medium_word = new Text();
		private Text long_word = new Text();
		private Text extra_long_word = new Text();
		
	
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper task of Lang Chen, s3754699");
					
			StringTokenizer itr = new StringTokenizer( value.toString());
			while (itr.hasMoreTokens()) {
				String next_token = itr.nextToken();
				Integer token_length = next_token.length();
				
				// Classify the words based on lengths
				if (token_length>=1 && token_length<=4) {
					short_word.set("short");
					context.write(short_word, one);	
				}
				if (token_length>=5 && token_length<=7) {
					medium_word.set("medium");
					context.write(medium_word, one);	
				}
				if (token_length>=8 && token_length<=10) {
					long_word.set("long");
					context.write(long_word, one);	
				}
				if (token_length>10) {
					extra_long_word.set("extra_long");
					context.write(extra_long_word, one);	
				}
		
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The reducer task of Lang Chen, s3754699");
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(task1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}