import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class fingerprintmapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException
	{
		int indexofhash = line.toString().indexOf("$$$");
		String docid = line.toString().substring(0, indexofhash).trim();
		String value = line.toString().substring(indexofhash+3, line.toString().length());
		context.write(new Text(docid), new Text(value));
	}
}
