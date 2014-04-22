import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.cedarsoftware.util.io.JsonReader;


public class ConceptualFilterMap1 extends Mapper<LongWritable, Text, Text, Text>
{
	Map<Integer, String> docmap;
	ArrayList<String> simhashuniques = new ArrayList<String>();
	
	public void setup(Context context) throws IOException, InterruptedException 
    {
    	Configuration conf = context.getConfiguration();
    	String mapstring = conf.get("map");
    	
		StringBuffer sf = new StringBuffer(mapstring);
		
		InputStream is = new ByteArrayInputStream(sf.toString().getBytes());
		
		JsonReader jsonreader = new JsonReader(is);
		Map<Integer, String> readMap = (HashMap)jsonreader.readObject();
		docmap = readMap;
		
		for(Integer key: readMap.keySet())
		{
			System.out.println(key + " -> " + readMap.get(key));
		}

		FileSystem hdfs = FileSystem.get(conf);
    	FileStatus[] partFiles = hdfs.listStatus(new Path("/home/sahil/deduplicationT"));
      
		for (FileStatus partFile : partFiles) 
		{
			if (! partFile.getPath().getName().startsWith("part-r")) 
			{
				continue;
			}
			
			FSDataInputStream fis = hdfs.open(partFile.getPath());
			BufferedReader bufferedreader = new BufferedReader(new InputStreamReader(fis));
			
			String docname = null;
			
			while ((docname = bufferedreader.readLine()) != null) //reads one doc name at a time
			{
				docname = docname.trim();
				simhashuniques.add(docname);
			}
			
			System.out.println("afadf  a  dfad" + simhashuniques);
			bufferedreader.close();
			fis.close();
		}
    }  
	
	public void map(LongWritable offset,Text line,Context context) throws IOException,InterruptedException
	{
		String record = line.toString();
		int startdollar = record.indexOf("$$$");
		int starthash = record.indexOf("###");
		
		int row = Integer.parseInt(record.substring(0, startdollar).trim());
		int column = Integer.parseInt(record.substring(startdollar+3, starthash).trim());
		int value = Integer.parseInt(record.substring(starthash+3).trim());
		
		System.out.println("adfafadf" + row + " " + column + " " + value +  " " + docmap.get(column));
		
		if(simhashuniques.contains(docmap.get(column)))
		{
			context.write(new Text(docmap.get(column)), new Text(row + " $$$ " + value));
		}
	}
	

}
