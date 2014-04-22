import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class docshavingtermmapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable termdoc,Text line,Context context) throws IOException, InterruptedException
	{
		String input = line.toString();
		
		int index = input.indexOf("$$$");
		String term = input.substring(0, index);

		String docindexfrequency = input.substring(index+3, input.length()).trim();

		context.write(new Text(term), new Text(docindexfrequency.trim()));
	}
}
