import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * tf * noofdocs (containing) /total docs
 */
public class tfidfreducer extends Reducer<Text, Text, Text, FloatWritable>
{
	public void reduce(Text docid,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		for(Text temp : values)
		{
			int indexofterm = temp.toString().indexOf("@@@");
			String term = temp.toString().substring(0, indexofterm).trim();
			
			int indexofhash = temp.toString().indexOf("###");
			StringTokenizer stringTokenizer =  new StringTokenizer(temp.toString().substring(indexofhash+3,temp.toString().length()));
			int frequency  = Integer.parseInt(stringTokenizer.nextToken());
			int inversefrequency  = Integer.parseInt(stringTokenizer.nextToken());
			
			context.write(new Text(new String(term + " $$$ " + docid + temp.toString().substring(indexofterm, indexofhash).trim())),
					
					new FloatWritable((float)(frequency * inversefrequency)/2));
		}
	}
}
