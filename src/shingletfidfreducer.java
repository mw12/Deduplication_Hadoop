import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class shingletfidfreducer extends Reducer<Text, Text, Text, Text>
{
	
	public static String SHA1(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException  
    { 
	    MessageDigest md = MessageDigest.getInstance("SHA-1");
	    byte[] sha1hash = new byte[40];
	    md.update(text.getBytes("iso-8859-1"), 0, text.length());
	    sha1hash = md.digest();
	
	    
	   	return hexToBinary(byteArrayToHexString(sha1hash).substring(0, 16));
    } 
	
	public static String byteArrayToHexString(byte[] b) {
		  String result = "";
		  for (int i=0; i < b.length; i++) {
		    result +=
		          Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
		  }
		  return result;
		}
	
	public static String hexToBinary(String hex)
	{
		StringBuffer binary = new StringBuffer();
		
		for(char c: hex.toCharArray())
		{
			switch(c)
			{
				case '0': binary.append("0000");break;
				case '1': binary.append("0001");break;
				case '2': binary.append("0010");break;
				case '3': binary.append("0011");break;
				case '4': binary.append("0100");break;
				case '5': binary.append("0101");break;
				case '6': binary.append("0110");break;
				case '7': binary.append("0111");break;
				case '8': binary.append("1000");break;
				case '9': binary.append("1001");break;
				case 'a': binary.append("1010");break;
				case 'b': binary.append("1011");break;
				case 'c': binary.append("1100");break;
				case 'd': binary.append("1101");break;
				case 'e': binary.append("1110");break;
				case 'f': binary.append("1111");break;
			}
		}
		return binary.toString();
	}
	
	public void reduce(Text docid,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		ArrayList<String> temp = new ArrayList<String>();
		for (Text text : value) 
		{
			temp.add(text.toString());
			//context.write(text, new Text());
		}
		//context.write(new Text(" " + temp.size()), new Text());
		Collections.sort(temp, new Comparator<String>() 
		{
			@Override
			public int compare(String o1, String o2) 
			{
				int indexofdollar = o1.indexOf("$$$");
				String index = o1.substring(0, indexofdollar).trim();
				
				int indexofdollar1 = o2.indexOf("$$$");
				String index1 = o2.substring(0, indexofdollar1).trim();
				
				return Integer.valueOf(index)-(Integer.valueOf(index1));//(Integer.valueOf(index1));
				
			}
			
		});
		
		int index=0;
		while(index<temp.size()-1)
		{
			String docid1 = docid.toString();//.substring(0,docid.toString().indexOf("$$$")).trim();
			String current = temp.get(index);
			int indexoftfidf = current.toString().indexOf("###");
			int indexofterm = current.toString().indexOf("$$$");
			String term1 = current.toString().substring(indexofterm+3, indexoftfidf).trim();
			Float tfidf = Float.parseFloat(current.toString().substring(indexoftfidf+3, current.toString().length()));
		
			String current1 = temp.get(index + 1);
			int indexoftfidf1 = current1.toString().indexOf("###");
			int indexofterm1 = current1.toString().indexOf("$$$");
			
			String term2 = current1.toString().substring(indexofterm1+3, indexoftfidf1).trim();
			Float tfidf1 = Float.parseFloat(current1.toString().substring(indexoftfidf1+3, current1.toString().length()));
			String shingle = term1  + " " + term2;
			String hashedshingle;
			try 
			{
				//context.write(new Text(shingle), new Text("sahil"));
				hashedshingle = SHA1(shingle);
				String shingletfidf = String.valueOf(tfidf1 + tfidf);
				context.write(new Text(docid1  + " $$$ "), new Text(hashedshingle + " ### " + shingletfidf));
			}
			catch (NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			}
				
			
			index++;
		}
		
	}
}
