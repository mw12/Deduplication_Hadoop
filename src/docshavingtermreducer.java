import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class docshavingtermreducer extends Reducer<Text, Text, Text, IntWritable>
{
	public void reduce(Text term,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		int count =0;
		String lastname = null;
		ArrayList<String> arrayList  = new ArrayList<String>();
		for(Text temp: values)
		{
			int indexofdoc = temp.toString().indexOf("@@@");
			String currentfilename = temp.toString().substring(0,indexofdoc).trim();
			//StringTokenizer stringTokenizer = new StringTokenizer(temp.toString(),"$");
			arrayList.add(term + " $$$ " + temp);//term + file ID + index + frequency
			if(lastname==null)
				count++;
			else if(currentfilename.compareTo(lastname)!=0)
				{
					//context.write(new Text(currentfilename + "," + lastname),new IntWritable(1000));
					count++;
				}
			lastname = currentfilename;
		}
		for(String temp : arrayList)
		{
			 context.write(new Text(temp),new IntWritable(count));
		}
	}
}
