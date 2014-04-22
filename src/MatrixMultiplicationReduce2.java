import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReduce2 extends Reducer<Text, FloatWritable, Text, FloatWritable>
{	
	public void reduce(Text indices,Iterable<FloatWritable> products,Context context) throws IOException, InterruptedException
	{
		float sum = 0;
		for(FloatWritable tempprod: products)
			sum+=tempprod.get();
		
		context.write(new Text(indices + " ### "), new FloatWritable(sum));
	}
}
