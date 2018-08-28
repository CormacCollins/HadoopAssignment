import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		int max = 0;
		int tempCount = 0;
		String maxWord = "";
		//Find the highest count for the current key(word) in the fileName
		while(it.hasNext()) {
			String value = it.next().toString();
			String[] vals = value.split(":");
			

			try {
				tempCount = Integer.parseInt(vals[1]);
				if(tempCount > max) {
					//set new max and store the file it is associated with
					max = tempCount;
					maxWord = vals[0];
				}
			}
			catch(Exception ex){
				String[] v = value.split(" ");
				tempCount = Integer.parseInt(v[1]);
				if(tempCount > max) {
					//set new max and store the file it is associated with
					max = tempCount;
					maxWord = v[0];
				}
			}
			
			
		}
		
		//output fileName and count
		Text res = new Text(maxWord + " " + Integer.toString(max));
		context.write(key, res);
	}
}