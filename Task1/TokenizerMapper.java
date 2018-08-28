import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// get input filename
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
		Text fName = new Text(fileName);
		String newWord = new String();
		Map<String, Integer> map = new HashMap<String, Integer>();

		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			//counting number of each word in this file
			newWord = (itr.nextToken());
			
			//strip out unnecessary characters
			if(newWord.contains(",")) {
				newWord = newWord.replace(",", "");
				System.out.println(newWord);
			}
			
			
			if (map.get(newWord) == null) {
				map.put(newWord, 1);
			} else {
				map.put(newWord, map.get(newWord) + 1);
			}
		}

		//write each word count corresponding to the unique 'fileName'
		for (Map.Entry<String, Integer> item : map.entrySet()) {
			String s = fName + ":" + item.getValue();
			Text fileAndWordCount = new Text(s);
			Text word = new Text(item.getKey().toString());
			context.write(word, fileAndWordCount);
		}
			
	}
}