import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConceptualFilterMap3 extends Mapper<LongWritable, Text, Text, Text> 
{   
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException
    {
    	String linestring = line.toString();
    	int startdollar = linestring.indexOf("$$$");
    	int starthash = linestring.indexOf("###");
    	int startat = linestring.indexOf("@@@");
    	int startpercent = linestring.indexOf("%%%");
    	
    	String docname1 = linestring.substring(0, startdollar).trim();
    	String docname2 = linestring.substring(startdollar+3, starthash).trim();
    	
    	String restofrecord = linestring.substring(starthash+3).trim();
    	
    	System.out.println("Rest of record: "+docname1 + " " + docname2 + " " + restofrecord);
    }
}
