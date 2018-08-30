
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		Map<String, float[]> map = new HashMap<String, float[]>();
		int count = 0;
		while(it.hasNext()) {
			String value = it.next().toString();
			String[] vals = value.split(":");
			
			String id = vals[0];
			String temp = vals[1];
			float tmp = Float.parseFloat(temp)/10;
			
			//increment the temperature and it's count (so we can avg at the end)
			//so we may have many avg temps for the same date (in year)
			if (map.get(id) == null) {
				map.put(id, new float[] {tmp, 1});
			} else {
				float[] arr = map.get(id);
				float newVal = arr[0] + tmp;
				float newCount = arr[1] + 1;
				map.put(id, new float[] {arr[0] + tmp, arr[1] + 1 });
			}
			count++;
		}
		
		
		//Extract each date-avg pair and write them for the current ID
		for(Map.Entry<String, float[]> item : map.entrySet()) {
			float[] data = item.getValue();
			
			//divide by 10 for temp back in cels
			float avg = ((data[0]) / data[1]);
			
			Text id = new Text(item.getKey());
			Text dateTemp= new Text(key.toString() + " " + Float.toString(avg));
			context.write(id, dateTemp);
		}
		
	}
	
}