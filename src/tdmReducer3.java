import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class tdmReducer3 extends Reducer<Text,Text, Text,Text>
{
	/*maps each unique word to its index in the corpus!!*/
	private Map<String,Integer> terms = new HashMap<String,Integer>();

    public void setup(Context context) throws IOException, InterruptedException 
    {
    	Configuration conf = context.getConfiguration();
    	FileSystem hdfs = FileSystem.get(conf);
    	FileStatus[] partFiles = hdfs.listStatus(new Path("/home/sahil/tdm2"));
      
		for (FileStatus partFile : partFiles) 
		{
			if (! partFile.getPath().getName().startsWith("part-r")) 
			{
				continue;
			}
			
			FSDataInputStream fis = hdfs.open(partFile.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			
			String line = null;
			int index, startdollar;
			String term;
			while ((line = reader.readLine()) != null) 
			{
				startdollar = line.indexOf("$$$");
				term = line.substring(0, startdollar).trim();
				index = Integer.parseInt(line.substring(startdollar+3).trim());
				
				terms.put(term, index);
			}
			
			reader.close();
			fis.close();
		}
	}
    
	public void reduce(Text docid, Iterable<Text> tuple, Context context) throws IOException, InterruptedException
	{
		int starthash, wordcount, wordindex;
		String word;
		
		int[] docvector = new int[terms.size()];
		
		for (Text text : tuple) 
		{
			starthash = text.toString().indexOf("###");
			word = text.toString().substring(0, starthash).trim();
			wordcount = Integer.parseInt(text.toString().substring(starthash+3).trim());
			wordindex = terms.get(word);
			docvector[wordindex] = wordcount;
		}
		
		context.write(docid, new Text(" $$$ " + Arrays.toString(docvector)));
	}
	
}
