
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		// always good to print some stuff
//		System.out.println("In Reducer ----------");
//		System.out.println("key: " + key.toString());
		Iterator<Text> it = values.iterator();

		// let's iterate through all the key-value pairs
		int tempSum = 0;
		float count = 0;

		while(it.hasNext()) {
			String value = it.next().toString();						
			tempSum += Integer.parseInt(value);
			count++;
		}
		
		
		String[] keys = key.toString().split(":");
		String id = keys[0];
		String year = keys[1];
		
		Text newKey = new Text(id + " " + year);
			
		float finalavg=(float)tempSum/count;
		Text v = new Text();
		v= new Text(Float.toString(finalavg));				
		context.write(newKey, v);
	}
	
}