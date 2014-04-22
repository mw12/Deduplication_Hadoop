import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConceptualFilterReduce2 extends Reducer<IntWritable, Text, Text, Text>
{	
	public void reduce(IntWritable partnumber, Iterable<Text> records, Context context) throws IOException, InterruptedException
	{
		Map <String, List> fingerprintmap = new HashMap<String, List>();
		int startdollar;
		String recordstring, fingerprintstring, docname;
		List<String> curfingerprint;
		List<String> alldocs = new ArrayList<String>();
		
		for(Text record: records)
		{
			recordstring = record.toString();
			startdollar = recordstring.indexOf("$$$");
			docname = recordstring.substring(0, startdollar).trim();
			alldocs.add(docname);
			fingerprintstring = recordstring.substring(startdollar+3).trim();
			curfingerprint = Arrays.asList(fingerprintstring.split(","));
			fingerprintmap.put(docname, curfingerprint);
		}
		
		for(int i=0; i<alldocs.size(); i++)
		{
			for(int j=i+1; j<alldocs.size(); j++)
			{
				String docname1 = alldocs.get(i);
				String docname2 = alldocs.get(j);
				List<String> fingerprint1 = fingerprintmap.get(docname1);
				List<String> fingerprint2 = fingerprintmap.get(docname2);
				int partialdotproduct = 0, partsquaredsum1 = 0, partsquaredsum2 = 0;
				
				for(int k=0; k<fingerprint1.size(); k++)
				{
					int val1 = Integer.parseInt(fingerprint1.get(k));
					int val2 = Integer.parseInt(fingerprint2.get(k));
					
					partsquaredsum1 += (val1*val1);
					partsquaredsum2 += (val2*val2);
					
					partialdotproduct += (val1*val2);
				}
				
				context.write(new Text(docname1 + " $$$ " + docname2 + " ### "), new Text(partialdotproduct + " @@@ " + partsquaredsum1 + " %%% " + partsquaredsum2));
			}
		}
	}
}
