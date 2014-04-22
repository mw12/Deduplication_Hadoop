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
	
	private static String convertToHex(byte[] data) 
	{ 
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < data.length; i++) { 
            int halfbyte = (data[i] >>> 4) & 0x0F;
            int two_halfs = 0;
            do { 
                if ((0 <= halfbyte) && (halfbyte <= 9)) 
                    buf.append((char) ('0' + halfbyte));
                else 
                    buf.append((char) ('a' + (halfbyte - 10)));
                halfbyte = data[i] & 0x0F;
            } while(two_halfs++ < 1);
        } 
        String hex64 = buf.toString();
        StringBuffer binary =  new StringBuffer(); 
        for (int i = 0; i < hex64.length(); i++) 
        {        	
        	int decimal = Integer.parseInt(String.valueOf(hex64.charAt(i)), 16);
        	binary.append(Integer.toBinaryString(decimal));
        	if(binary.length()>=64)
        		return binary.toString().substring(0,64);
        	
		}
        return binary.toString();
        //return buf.toString();
    } 
 
    public static String SHA1(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException  
    { 
	    MessageDigest md = MessageDigest.getInstance("SHA-1");
	    byte[] sha1hash = new byte[40];
	    md.update(text.getBytes("iso-8859-1"), 0, text.length());
	    sha1hash = md.digest();
	    return convertToHex(sha1hash);
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
