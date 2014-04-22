import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReduce2 extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	public void reduce(Text indices,Iterable<IntWritable> products,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		for(IntWritable tempprod: products)
			sum+=tempprod.get();
		
		context.write(new Text(indices + " ### "), new IntWritable(sum));
	}
}
