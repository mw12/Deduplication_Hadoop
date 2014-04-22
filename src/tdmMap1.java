import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class tdmMap1 extends Mapper<LongWritable, Text, Text, IntWritable> 
{
    
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException
    {
    	Path filepath = ((FileSplit)context.getInputSplit()).getPath();
		String filename = filepath.getName();
		StringTokenizer stringTokenizer =  new StringTokenizer(line.toString());
		while(stringTokenizer.hasMoreTokens())
		  {
			  String key = filename + " $$$ " + stringTokenizer.nextToken();
			  context.write(new Text(key), new IntWritable(1));
		  }
    }
}
