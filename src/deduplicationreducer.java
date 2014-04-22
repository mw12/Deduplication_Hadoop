import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class deduplicationreducer extends Reducer<IntWritable, Text, Text, Text>
{
	
	private static ArrayList<String> uniquedocs = new ArrayList<String>();
	private static ArrayList<String> duplicatedocs = new ArrayList<String>();
	public int hammingdistance(String fingerprint1,String fingerprint2)//similar bits
	{
		int hammingdistance = 0;
		for (int i = 0; i < fingerprint1.length(); i++) 
		{
			if(fingerprint1.charAt(i)==fingerprint2.charAt(i))
			{
				hammingdistance++;
			}
		}
		return hammingdistance;
	}

	public void reduce(IntWritable rotationnumber,Iterable<Text> fingerdoc,Context context) throws IOException, InterruptedException
	{
		
		Iterator<Text> iter = fingerdoc.iterator();
		String previous,fingerprint1 = null,docid1 = null;
		if(iter.hasNext())
		{
			String value = iter.next().toString();
			int delimitter = value.indexOf("$$$");
			fingerprint1= value.substring(0,delimitter).trim();
			docid1 = value.substring(delimitter+3,value.length()).trim();
		}
		while(iter.hasNext())
		{
			String current = iter.next().toString();
			int delimitter = current.indexOf("$$$");
			String fingerprint2 = current.substring(0,delimitter).trim();
			String docid2 = current.substring(delimitter+3,current.length()).trim();
			
			int similarbits = hammingdistance(fingerprint1, fingerprint2);
			if(similarbits > 45)
			{
				if(!uniquedocs.contains(docid1) && !duplicatedocs.contains(docid1))
				{
					uniquedocs.add(docid1);
				}
				if(!duplicatedocs.contains(docid2) && !uniquedocs.contains(docid2))
				{
					duplicatedocs.add(docid2);
				}
				
			}
			else
			{
				if(!uniquedocs.contains(docid1) && !duplicatedocs.contains(docid1))
				{
					uniquedocs.add(docid1);
				}
				if(!uniquedocs.contains(docid1) && !duplicatedocs.contains(docid1))
				{
					uniquedocs.add(docid2);
				}
				
			}
			
			docid1 = docid2;
			fingerprint1 = fingerprint2;
			
		}

		if(rotationnumber.get() == 63)
		{
			for(String docname: uniquedocs)
				context.write(new Text(docname), null); //CHANGED HERE NOT YET CHECKED!!!!!
		}
		
	}
}
