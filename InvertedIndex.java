package org.veber.udacityhadoop.invertedindex;

import java.util.Arrays;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
* MapReduce program that analyzes forum data and displays word count for a given word and ID list of
* users who use the a specific word.  This performs filtering and Inverted Indexing
*/

public class InvertedIndex {
	private final static IntWritable ONE = new IntWritable(1);
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		  throw new Exception("Usage: <input directory> <output directory>");
		}
		Configuration config = new Configuration();
		Job job = new Job(config,"hadoop weblog");
		job.setJarByClass(InvertedIndex.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
    /** Mapper for Inverted Index.
     *
     * The class Mapper is parameterized by
     * <in key type, in value type, out key type, out value type>.
     *
     * Thus, this mapper takes (LongWritable key, Text value) pairs and outputs
     * (Text key, Text value) pairs. The input keys are assumed
     * to be identifiers for documents, which are ignored, and the values
     * to be the content of documents. The output keys are words found
     * within each forum post, and the output values are the user id.
     */
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text id = new Text();
		
        /** Actual map function. Takes one document's text and emits key-value
         * pairs for each word found in the document.
         *
         * @param key Document identifier (ignored).
         * @param value a line of the current document.
         * @param context MapperContext object for accessing output, 
         *                configuration information, etc.
         */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] data = line.split("\t");
			String tempWord = "";
			//If only certain words are of interest this will reduce the amount of mapper outputs
			String mapShrink = ""; 
			if (data.length > 1){
				id.set(data[0]);
				String[] words = data[4].split("[\\W]+");
				// String[] words = data[4].split("[.\\,\\!\\?\\:\\;\\\"\\(\\)\\<\\>\\[\\]\\#\\$\\=\\-\\/\\s\\\n\\\t\\|]+");
				for (String word : words){
					tempWord = word.toLowerCase();
					if (tempWord.contains(mapShrink)) {
						context.write(new Text(tempWord),id);
					}
				}
			}
		}	
	}
    /** Reducer for Inverted Index.
     *
     * Like the Mapper base class, the base class Reducer is parameterized by 
     * <in key type, in value type, out key type, out value type>.
     *
     * For each Text key, which represents a word, this reducer gets a list of
     * Text values, computes the sum of those values, and the key-value
     * pair (word, sum).
     */
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
        /** Actual reduce function.
         * 
         * @param key Word.
         * @param values Iterator over the values for this key.
         * @param context ReducerContext object for accessing output,
         *                configuration information, etc.
         */		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			String fantastically = "fantastically";
			String nodes = "";
			String word = key.toString();
			for (Text value : values) {
				sum ++;
				nodes+=value.toString()+", ";
			}
			if (word.equals("fantastic")){
				context.write(key, new Text(Integer.toString(sum)));
			}
			if (word.equals(fantastically)){
				context.write(key,new Text(nodes));
			}
		}
	}
}