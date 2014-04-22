import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class tfidfmapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException
	{
		String input = line.toString();
		int index = input.indexOf("$$$");
		String term = input.substring(0, index);
		
		int index1 = input.indexOf("@@@");
		String docid = input.substring(index+3, index1).trim();
		
		String value = term.trim() + " " + input.substring(index1, input.length());
		context.write(new Text(docid), new Text(value));
	}
}
