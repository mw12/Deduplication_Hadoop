import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class fingerprintreducer extends Reducer<Text, Text, Text, Text>
{
	public void reduce(Text docid,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		ArrayList<String> hashes = new ArrayList<String>();
		ArrayList<Float> tfidfs = new ArrayList<Float>();
		for (Text string : value) 
		{
			int indexoftfidf = string.toString().indexOf("###");
			tfidfs.add(Float.parseFloat(string.toString().substring(indexoftfidf+3,string.toString().length()).trim()));
			
			hashes.add(string.toString().substring(0, indexoftfidf).trim());
		}
		//context.write(new Text(Arrays.toString(tfidfs.toArray())), new Text("sfas"));
		StringBuffer buffer = new StringBuffer();
		Integer fingerprint[] = new Integer[64]; 
		for(int i=0;i<64;i++)
		{
			float currentweightssum = (float)0;
			for (int j = 0; j < hashes.size(); j++) 
			{
				//context.write(new Text(String.valueOf(hashes.get(j).charAt(i))), new Text());
				if(Integer.parseInt(String.valueOf(hashes.get(j).charAt(i)))==1)
				{
					currentweightssum += tfidfs.get(j);
				}
				else
				{
					//context.write(new Text("here"),null);
					currentweightssum -= tfidfs.get(j);
				}
			}
			//context.write(new Text(String.valueOf(currentweightssum)), new Text());
			if((int)currentweightssum>=0)
			{
				fingerprint[i] = 1;
				buffer.append('1');
				
			}
			else
				{
				fingerprint[i] = 0;
				buffer.append('0');
				}
				
		}
		//context.write(new Text(String.valueOf(buffer.length())), null);
		context.write(new Text(buffer.toString() + " $$$ "),new Text(docid.toString()));

	}
}
