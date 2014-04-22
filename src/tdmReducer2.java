import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class tdmReducer2 extends Reducer<Text,IntWritable, Text,IntWritable>
{
	private static int index = 0;
	
	public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException
	{
		context.write(new Text(key + " $$$ "), new IntWritable(index));
		index++;
	}
	
}
