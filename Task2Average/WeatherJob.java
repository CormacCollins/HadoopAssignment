import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.fs.Path;


public class WeatherJob {


	public static void main(String[] args) throws Exception {
		
		String input = args[0];
		String output = args[1];

	    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "secondary sort");
		
		job.setJarByClass(WeatherJob.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); 
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		// TextInputFormat will make sure that we are fed the text file line by line
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WeatherMapper.class);
		job.setReducerClass(WeatherReducer.class);
				
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}