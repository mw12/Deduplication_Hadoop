import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReduce1 extends Reducer<IntWritable, Text, Text, IntWritable>
{	
	public void reduce(IntWritable index,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		ArrayList<int[]> matrix1Records = new ArrayList<int[]>();
		ArrayList<int[]> matrix2Records = new ArrayList<int[]>();
		
		for(Text t: value)
		{
			String tuple = t.toString();
			int indexofdollar = tuple.indexOf("$$$");
			int indexofhash = tuple.indexOf("###");
			int element = Integer.parseInt(tuple.substring(indexofdollar+3, indexofhash).trim());
			int firstarg = Integer.parseInt(tuple.substring(0, indexofdollar).trim());
			int secondarg = index.get();
			
			int[] temprecord = new int[3];
			
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
		
		for(int[] record1: matrix1Records)
		{
			for(int[] record2: matrix2Records)
			{
				int result = record1[2]*record2[2];
				String outputkey = record1[0] + " $$$ " + record2[1] + " ### ";
				context.write(new Text(outputkey), new IntWritable(result));
			}
		}
	}
}
