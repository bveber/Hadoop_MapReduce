package org.veber.udacityhadoop.meansales;

import java.util.Arrays;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
// import java.util.Date;

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
* MapReduce program that analyzes sales data and displays the average cost per sale
* separately for each day of the week.  This performs average (numerical summarization).
*/

public class MeanSales {
	private final static IntWritable ONE = new IntWritable(1);
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		  throw new Exception("Usage: <input directory> <output directory>");
		}
		Configuration config = new Configuration();
		Job job = new Job(config,"hadoop weblog");
		job.setJarByClass(MeanSales.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MeanSalesMapper.class);
		job.setReducerClass(MeanSalesReducer.class);
		
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
     * to be the content of documents. The output keys are the day of the week
     * and the output values are the user sales.
     */
	public static class MeanSalesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text day = new Text();
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		private Calendar cal = Calendar.getInstance();
		
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

			if (data.length == 6){
				String[] date_split = data[0].split("-");
				// Date date = new Date(Integer.parseInt(date_split[0]),Integer.parseInt(date_split[1]),Integer.parseInt(date_split[2]));
				cal.set(Integer.parseInt(date_split[0]),Integer.parseInt(date_split[1])-1,Integer.parseInt(date_split[2]));
				String day = Integer.toString(cal.get(Calendar.DAY_OF_WEEK));
				String sale = data[4];
				context.write(new Text(day),new Text(sale));
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
	public static class MeanSalesReducer extends Reducer<Text, Text, Text, Text>{
        /** Actual reduce function.
         * 
         * @param key Day of week (1=Sunday).
         * @param values Iterator over the values for this key.
         * @param context ReducerContext object for accessing output,
         *                configuration information, etc.
         */		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			double counter = 0.0;
			for (Text value : values) {
				sum += Double.parseDouble(value.toString());
				counter += 1.0;
			}
			double mean = sum/counter;
			context.write(key,new Text(String.valueOf(mean)));
			
		}
	}
}