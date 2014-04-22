package conceptual;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SingularValueDecomposition;
import org.apache.mahout.math.Vector;


public class MatrixImplementor {
	
	public Map<Integer, String> docmap = new HashMap<Integer, String>();
	
	public void writetofile(Matrix m, Path filepath, Configuration conf) throws IOException
	{
		FileSystem hdfs = FileSystem.get(conf);
		
		if(hdfs.exists(filepath))
			hdfs.delete(filepath);
		
		OutputStream fis = hdfs.create(filepath);//open(filepath);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fis));
		int rows = m.rowSize();
		int columns = m.columnSize();
		System.out.println("rows are " + rows);
		for(int row=0; row<rows; row++)
		{
			for(int column=0; column<columns; column++)
				{
					if(column+1==columns)
					{
						writer.write(String.valueOf((int)(m.get(row, column))));//);
					}
					else
					{
						writer.write(String.valueOf((int)(m.get(row, column))) + ",");//);
					}
					
					//System.out.println;
				}
			writer.write("\n");
		}
		writer.close();
		fis.close();
		hdfs.close();
	}
	private int getnumberoflines(Path filepath,Configuration conf) throws IOException
	{
		//System.out.println("getnumber of lines: "+filepath);
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream fis = hdfs.open(filepath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		
		LineNumberReader  lnr = new LineNumberReader(reader);//new FileReader(new File(filepath)));
		lnr.skip(Long.MAX_VALUE);
		lnr.close();
		return lnr.getLineNumber();
	}
	private int getnumberofdocs(String rootdir, Configuration conf) throws IOException
	{
		FileSystem hdfs = FileSystem.get(conf);
    	FileStatus[] partFiles = hdfs.listStatus(new Path(rootdir));
    	    
    	int totaldocs = 0;
    	
    	for (FileStatus partFile : partFiles) 
		{
			if (! partFile.getPath().getName().startsWith("part-r")) 
			{
				continue;
			}
			
			FSDataInputStream fis = hdfs.open(partFile.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			
			totaldocs += getnumberoflines(partFile.getPath(),conf);
			
			reader.close();
			fis.close();
		}
    	
    	return totaldocs;
	}
	private int getwords(String path, Configuration conf) throws FileNotFoundException, IllegalArgumentException, IOException 
	{		
		FileSystem hdfs = FileSystem.get(conf);
    	FileStatus[] partFiles = hdfs.listStatus(new Path(path));
    	        	
    	for (FileStatus partFile : partFiles) 
		{
			if (! partFile.getPath().getName().startsWith("part-r")) 
			{
				continue;
			}
			
			FSDataInputStream fis = hdfs.open(partFile.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		
			String line = reader.readLine(); 
			int openbracket = line.indexOf("[");
			int closebracket = line.indexOf("]");
			
			String wordstring = line.substring(openbracket+1, closebracket);
			
			List<String> wordlist = Arrays.asList(wordstring.split(","));
			
			reader.close();
			fis.close();
			return wordlist.size();
		}
    	
    	
		return -1;
	}
	public DenseMatrix buildmatrix(String path, Configuration conf) throws FileNotFoundException, IllegalArgumentException, IOException
	{
    	FileSystem hdfs = FileSystem.get(conf);
    	FileStatus[] partFiles = hdfs.listStatus(new Path(path));
    	
    	int totaldocs = getnumberofdocs(path, conf);
    	int uniquewords = getwords(path, conf);
    	
    	DenseMatrix dm = new DenseMatrix(uniquewords, totaldocs);
    	System.out.println("unique words " + uniquewords + "  " + " total docs " + totaldocs);
    	
    	for (FileStatus partFile : partFiles) 
		{
			if (! partFile.getPath().getName().startsWith("part-r")) 
			{
				continue;
			}
			
			FSDataInputStream fis = hdfs.open(partFile.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			
			String line = null, wordstring, docname;
			int openbracket, closebracket, startdollar;
			List<String> wordlist;
	    	int column = 0, docid = 0;
	    	
			
			while ((line = reader.readLine()) != null) 
			{
				openbracket = line.indexOf("[");
				closebracket = line.indexOf("]");
				startdollar = line.indexOf("$$$");
				
				docname = line.substring(0, startdollar).trim();
				docmap.put(docid++, docname);
				
				wordstring = line.substring(openbracket+1, closebracket);
				wordlist = Arrays.asList(wordstring.split(","));
				
		    	double[] docvectorarray = new double[uniquewords];
				Vector docvector = new DenseVector(uniquewords);

				int index = 0;
				for(String occurrence: wordlist)
				{
					docvectorarray[index] = Integer.parseInt(occurrence.trim());
					index++;
				}
				
				
				docvector.assign(docvectorarray);
				
				dm.assignColumn(column, docvector);
				column++;
				
			}
			
			reader.close();
			fis.close();
		}
    	System.out.print(dm.toString());
    	return dm;
	}
	public SingularValueDecomposition getsvd(DenseMatrix dm)
	{
		return new SingularValueDecomposition(dm);
	}
}
