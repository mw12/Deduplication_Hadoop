import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class shinglereducer extends Reducer<Text,IntWritable, Text,IntWritable>
{
	public void reduce(Text tuple,Iterable<IntWritable> indexes,Context context) throws IOException, InterruptedException
	{
		int count=0;
		ArrayList<Integer> index = new ArrayList<Integer>();
		Iterator<IntWritable> iter = indexes.iterator();
		while(iter.hasNext())
		{
			//IntWritable t = new IntWritable(iter.next().get());
			
			index.add(iter.next().get());
			count++;
			
		}
		for (Integer temp : index) 
		{
			context.write(new Text(tuple.toString() + " @@@ " + temp + " ### "), new IntWritable(count));
		}
		
	}
}
