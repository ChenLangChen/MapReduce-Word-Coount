package BigData.Assignment1.simpleWordCount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

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


//Write a MapReduce program that outputs a count of all words that begin 
//with a vowel and count of all how many words that begin with a consonant.

public class task2 {
	public static final Logger LOG = Logger.getLogger(task2.class);
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);		
		private Text vowel_word = new Text();
		private Text consonant_word = new Text();
		
		// Function to check whether a word starts with vowel or consonant.
		public static String check_VC(String word){
			// Extract the first character of a word
	        String first_char = Character.toString(word.charAt(0));
	        
	        Pattern vowel_pattern = Pattern.compile("[aeiou]",Pattern.CASE_INSENSITIVE);
	        Pattern consonant_pattern = Pattern.compile("[BCDFGHJKLMNPQRSTVXZWY]",Pattern.CASE_INSENSITIVE);
	        
	        boolean vowel = vowel_pattern.matcher(first_char).matches();
	        boolean consonant = consonant_pattern.matcher(first_char).matches();

	        if (vowel){
	            return "vowel";
	        }
	        if (consonant){
	            return "consonant";
	        }
			return "";
	    }

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper task of Lang Chen, s3754699");
		
			StringTokenizer itr = new StringTokenizer( value.toString());
			while (itr.hasMoreTokens()) {
				String next_token = itr.nextToken();
				
				if (check_VC(next_token)=="vowel") {
					vowel_word.set("vowel");
					context.write(vowel_word, one);					
				}
				if (check_VC(next_token)=="consonant") {
					consonant_word.set("consonant");
					context.write(consonant_word, one);
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
		job.setJarByClass(task2.class);
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