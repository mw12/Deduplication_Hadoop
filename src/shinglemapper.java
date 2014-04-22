import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class shinglemapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public static int index = 0;
	public void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException
	{
		Path filepath = ((FileSplit)context.getInputSplit()).getPath();
		String filename = filepath.getName();
		StringTokenizer stringTokenizer =  new StringTokenizer(line.toString());
		while(stringTokenizer.hasMoreTokens())
		  {
			  String t1 = stringTokenizer.nextToken() + " $$$ " + filename;
			  context.write(new Text(t1), new IntWritable(index));
			  index++;
		  }
//		while(true)
//		{
//			ArrayList<Text> tuple = new ArrayList<Text>();
//			  if(stringTokenizer.hasMoreTokens())
//			  {
//				  String t1 = stringTokenizer.nextToken();
//				  String t2, result;
//				  if(stringTokenizer.hasMoreTokens())
//				  {
//					  t2 = stringTokenizer.nextToken();
//					  result = t1 + " " + t2 + "$" + filename;
//					  context.write(new Text(result), new IntWritable(1));
//				  }
//				  else
//				  {
//					  result = t1 + "$" + filename;
//					  context.write(new Text(result), new IntWritable(1));
//				  }				  
//			  }
//			  else
//				  break;
//		}
		
	}
}
