import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class tdmMapper3 extends Mapper<LongWritable, Text, Text, Text> 
{   
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException
    {
    	int startdollar = line.toString().indexOf("$$$");
    	
    	String docid = line.toString().substring(0, startdollar).trim();
    	String value = line.toString().substring(startdollar+3).trim();
    	
    	//System.out.println(docid + " "+ value);
    	context.write(new Text(docid), new Text(value));
    }
}
