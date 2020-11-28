package BigData.Assignment1.simpleWordCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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


//Write a MapReduce program to count the number of each word where 
//the in-mapper combining is implemented rather than an independent combiner.

public class task3 {
	public static final Logger LOG = Logger.getLogger(task3.class);
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper task of Lang Chen, s3754699");
			
			// Create a HashMap for the in-mapper combiner. 
			HashMap<Text, Integer> hm = new HashMap<>();		
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				Text word = new Text();
				word.set(itr.nextToken());
				if (!hm.containsKey(word)) {
					hm.put(word, 1);
				}
				else {
					hm.put(word, hm.get(word)+1);
				}			
			}
			
			// Spitting out <Word : count> from Hashmap hm
			for (Map.Entry item: hm.entrySet()) {
				Integer count = (Integer) item.getValue();
				IntWritable count_writable = new IntWritable(count);
				context.write((Text) item.getKey(), count_writable);			
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
		job.setJarByClass(task3.class);
		job.setMapperClass(TokenizerMapper.class);
		// TODO 
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

