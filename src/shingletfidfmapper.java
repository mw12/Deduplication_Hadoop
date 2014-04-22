import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class shingletfidfmapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable offset,Text line,Context context) throws IOException, InterruptedException
	{
		int indexofterm = line.toString().indexOf("$$$");
		String term = line.toString().substring(0, indexofterm).trim();
		int indexofdoc = line.toString().indexOf("@@@");
		String docid = line.toString().substring(indexofterm+3, indexofdoc).trim();
		
		StringTokenizer stringTokenizer = new StringTokenizer(line.toString().substring((indexofdoc+3),line.toString().length()));
		int index = Integer.parseInt(stringTokenizer.nextToken());
		Float tfidf = Float.parseFloat(stringTokenizer.nextToken());
		
		String key = docid;
		String value = index + " $$$ " + term  + " ### " + tfidf;
		context.write(new Text(key), new Text(value));
	}
}
