import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MatrixMultiplicationMap2 extends Mapper<LongWritable, Text, Text, IntWritable> 
{	
	public void map(LongWritable offset,Text line,Context context) throws IOException,InterruptedException
	{
		String record = line.toString();
		int startdollar = record.indexOf("$$$");
		int starthash = record.indexOf("###");
		int row = Integer.parseInt(record.substring(0, startdollar).trim());
		int col = Integer.parseInt(record.substring(startdollar+3, starthash).trim());
		int product = Integer.parseInt(record.substring(starthash+3).trim());
		
		System.out.println("Now writing row: "+row+" $$$ "+col+" "+product);
		context.write(new Text(row+" $$$ "+col), new IntWritable(product));
	}
}
