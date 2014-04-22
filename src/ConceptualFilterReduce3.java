import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConceptualFilterReduce3 extends Reducer<Text, Text, Text, Text>
{	
	private static ArrayList<String> conceptualuniques = new ArrayList<String>(); 
	private static ArrayList<String> conceptualduplicates = new ArrayList<String>();
	
	public void reduce(Text docpair, Iterable<Text> records, Context context) throws IOException, InterruptedException
	{
		String recordstring, docpairstring;
		
		docpairstring = docpair.toString();
		
		int startdollar = docpairstring.indexOf("$$$");
		String docname1 = docpairstring.substring(0, startdollar).trim();
		String docname2 = docpairstring.substring(startdollar+3).trim();
		
		int startat, startpercent, dotproduct = 0, squaredsumdoc1 = 0, squaredsumdoc2 = 0;
		
		for(Text record: records)
		{
			recordstring = record.toString();
			startat = recordstring.indexOf("@@@");
			startpercent = recordstring.indexOf("%%%");
			
			dotproduct += Integer.parseInt(recordstring.substring(0, startat).trim());
			squaredsumdoc1 += Integer.parseInt(recordstring.substring(startat+3, startpercent).trim());
			squaredsumdoc2 += Integer.parseInt(recordstring.substring(startpercent+3).trim());	
		}
		
		double moddoc1 = Math.sqrt(squaredsumdoc1);
		double moddoc2 = Math.sqrt(squaredsumdoc2);
		
		if(dotproduct/(moddoc1*moddoc2) > 0.9)
		{
			if((!conceptualuniques.contains(docname1)) && (!conceptualduplicates.contains(docname1)))
				conceptualuniques.add(docname1);
			
			if((!conceptualuniques.contains(docname2)) && (!conceptualduplicates.contains(docname2)))
				conceptualduplicates.add(docname2);
		}
		
		context.write(new Text(conceptualuniques.toString()), new Text("crap"));
	}
}
