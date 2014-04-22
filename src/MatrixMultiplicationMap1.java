import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MatrixMultiplicationMap1 extends Mapper<LongWritable, Text, IntWritable, Text> 
{
	private static int rowMat1 = 0, rowMat2 = 0;
	
	public void map(LongWritable offset,Text line,Context context) throws IOException,InterruptedException
	{
		Path filepath = ((FileSplit)context.getInputSplit()).getPath();
		String filename = filepath.getName();
		int column = 0;
				
		StringTokenizer st = new StringTokenizer(line.toString(), ",");
		
		while(st.hasMoreTokens())
		{
			int element = Integer.parseInt(st.nextToken());
			if(filename.equalsIgnoreCase("matrix1"))
			{
				context.write(new IntWritable(column), new Text(rowMat1+" $$$ "+element+" ### "+"matrix1"));
				System.out.println(column+" " +rowMat1+" "+rowMat2+" "+element);
			}
			else
			{
				context.write(new IntWritable(rowMat2), new Text(column+" $$$ "+element+ " ### " + "matrix2"));
				System.out.println(column+" " +rowMat1+" "+rowMat2+" "+element);
			}
			
			column++;
		}
		
		if(filename.equalsIgnoreCase("matrix1"))
			rowMat1++;
		else
			rowMat2++;
		
	}
}
