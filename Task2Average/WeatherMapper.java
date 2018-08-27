
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// read the input - which is a complete row of the 17xy.csv file
		// split it with "," as delimiter
		String[] fields = value.toString().split(",");
		
		// extract the station-id
		Text stationId = new Text(fields[0].trim());
		
		// extract the temperature type 
		String tempType = fields[2].trim();
		
		// extract the date. Have to convert it from Long to String
		// you can prob. do better than me :)
		String date = Long.toString(Long.parseLong(fields[1].substring(0, 4)));
		String temp = Integer.toString(Integer.parseInt(fields[3].trim()));

		// We should only collect the Maximum temperature for this mapreduce job
		//if(new String("TMAX").equals(tempType)){
		Text v= new Text(temp);

		// write the output (key, value) pair
		// note that stationId is of form Text (as is v)
		Text idDate = new Text(fields[0].trim() + ":" + date);
		context.write(idDate, v);
		
		
	}
}