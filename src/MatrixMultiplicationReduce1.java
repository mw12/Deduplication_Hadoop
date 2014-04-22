import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReduce1 extends Reducer<IntWritable, Text, Text, FloatWritable>
{	
	public void reduce(IntWritable index,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		ArrayList<float[]> matrix1Records = new ArrayList<float[]>();
		ArrayList<float[]> matrix2Records = new ArrayList<float[]>();
		
		for(Text t: value)
		{
			String tuple = t.toString();
			int indexofdollar = tuple.indexOf("$$$");
			int indexofhash = tuple.indexOf("###");
			float element = Float.parseFloat(tuple.substring(indexofdollar+3, indexofhash).trim());
			int firstarg = Integer.parseInt(tuple.substring(0, indexofdollar).trim());
			int secondarg = index.get();
			
			float[] temprecord = new float[3];
			
			if(tuple.contains("matrix1"))
			{
				temprecord[0] = firstarg;
				temprecord[1] = secondarg;
				temprecord[2] = element;
				matrix1Records.add(temprecord);
			}
			else
			{
				temprecord[0] = secondarg;
				temprecord[1] = firstarg;
				temprecord[2] = element;
				matrix2Records.add(temprecord);
			}				
		}
		
		for(float[] record1: matrix1Records)
		{
			for(float[] record2: matrix2Records)
			{
				float result = record1[2]*record2[2];
				String outputkey = (int)record1[0] + " $$$ " + (int)record2[1] + " ### ";
				context.write(new Text(outputkey), new FloatWritable(result));
			}
		}
	}
}
