import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class tdmMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> 
{   
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException
    {
    	int startdollar = line.toString().indexOf("$$$");
    	int starthash = line.toString().indexOf("###");
    	
    	String word = line.toString().substring(startdollar+3, starthash).trim();
    	
    	context.write(new Text(word), new IntWritable(1));
    }
}
