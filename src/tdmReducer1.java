import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class tdmReducer1 extends Reducer<Text,IntWritable, Text,IntWritable>
{
	public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		
		for (IntWritable intWritable : counts) {
			sum++;
		}
		context.write(new Text(key.toString() + " ### "), new IntWritable(sum));
	}
	
}
