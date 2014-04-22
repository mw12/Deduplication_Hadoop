import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConceptualFilterMap2 extends Mapper<LongWritable, Text, IntWritable, Text> 
{   
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException
    {
    	String record = line.toString();
    	int startdollar = record.indexOf("$$$");
    	String docname = record.substring(0, startdollar).trim();
    	String fingerprintstring = record.substring(startdollar+3).trim();
    	List<String> fingerprintlist = Arrays.asList(fingerprintstring.split(","));
    	int length = fingerprintlist.size();
    	String fingerprintfirsthalf = "", fingerprintsecondhalf = "";
    	boolean firsttime = true;
    	
    	for(int i=0; i<length/2; i++)
    	{
    		if(firsttime)
    		{
    			fingerprintfirsthalf += fingerprintlist.get(i);
    			firsttime = false;
    		}
    		else
    			fingerprintfirsthalf += ","+fingerprintlist.get(i);
    	}
    	    	
    	firsttime = true;
    	for(int j=length/2; j<length; j++)
    	{
    		if(firsttime)
    		{
    			fingerprintsecondhalf += fingerprintlist.get(j);
    			firsttime = false;
    		}
    		else
    			fingerprintsecondhalf += ","+fingerprintlist.get(j);
    	}
    	
    	System.out.println("Outputing : " + docname + " $$$ " + fingerprintfirsthalf);
    	System.out.println("Outputing : " + docname + " $$$ " + fingerprintsecondhalf);
    	
    	context.write(new IntWritable(1), new Text(docname + " $$$ " + fingerprintfirsthalf));
    	context.write(new IntWritable(2), new Text(docname + " $$$ " + fingerprintsecondhalf));
    		
    }
}
