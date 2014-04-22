import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SingularValueDecomposition;

import com.cedarsoftware.util.io.JsonWriter;

import conceptual.MatrixImplementor;

public class Simhash implements Tool
{
	public int run(String[] args) throws Exception 
	{
		/**
		 * ******** shinglecount**********
		 **/
		Job shinglecount =  Job.getInstance(getConf(),"shinglecount");

		shinglecount.setMapperClass(shinglemapper.class);
		shinglecount.setReducerClass(shinglereducer.class);
		
		shinglecount.setOutputKeyClass(Text.class);
		shinglecount.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(shinglecount, new Path(args[0]));
	    FileOutputFormat.setOutputPath(shinglecount, new Path(args[1]));
	    
	    /*
		 * ******** docshavingterm**********
		 * calculating no of documents that contain that term
		 */
	    
	    Job docshavingterm = Job.getInstance(getConf(), "docscontaingword");
	    
	    docshavingterm.setMapperClass(docshavingtermmapper.class);
	    docshavingterm.setReducerClass(docshavingtermreducer.class);
	    
	    docshavingterm.setMapOutputKeyClass(Text.class);
	    docshavingterm.setMapOutputValueClass(Text.class);
	    
	    docshavingterm.setOutputKeyClass(Text.class);
	    docshavingterm.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(docshavingterm, new Path(args[1]));
	    FileOutputFormat.setOutputPath(docshavingterm, new Path(args[2]));
	    
	    /*
		 * ******** termstfidf**********
		 * tfidf computation of the term
		 */
	    
	    Job tfidf = Job.getInstance(getConf(), "tfidf computation");
	    
	    tfidf.setMapperClass(tfidfmapper.class);
	    tfidf.setReducerClass(tfidfreducer.class);
	    
	    tfidf.setMapOutputKeyClass(Text.class);
	    tfidf.setMapOutputValueClass(Text.class);
	    
	    tfidf.setOutputKeyClass(Text.class);
	    tfidf.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(tfidf, new Path(args[2]));
	    FileOutputFormat.setOutputPath(tfidf, new Path(args[3]));
	    
	    /*
		 * ******** shingletfidf**********
		 * shingling and hashing them
		 */
	    
	    
	    Job shingletfidf = Job.getInstance(getConf(), "shingle tfidf");
	    
	    shingletfidf.setMapperClass(shingletfidfmapper.class);
	    shingletfidf.setReducerClass(shingletfidfreducer.class);
	    
	    shingletfidf.setOutputKeyClass(Text.class);
	    shingletfidf.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(shingletfidf, new Path(args[3]));
	    FileOutputFormat.setOutputPath(shingletfidf, new Path(args[4]));

	    
	    /*
		 * ******** fingerprint**********
		 *
        */
	    
	    
	    Job fingerprint = Job.getInstance(getConf(), "docfingerprint");
	    
	    fingerprint.setMapperClass(fingerprintmapper.class);
	    fingerprint.setReducerClass(fingerprintreducer.class);
	    
	    fingerprint.setOutputKeyClass(Text.class);
	    fingerprint.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(fingerprint, new Path(args[4]));
	    FileOutputFormat.setOutputPath(fingerprint, new Path(args[5]));
	    
	    
	    /*
	     *	TEXTUALLY UNIQUE FOUND
	     */
		
	    Job deduplication  = Job.getInstance(getConf(), "deduplication");
	    
	    deduplication.setMapperClass(deduplicationmapper.class);
	    deduplication.setReducerClass(deduplicationreducer.class);
	    
	    deduplication.setMapOutputKeyClass(IntWritable.class);
	    deduplication.setMapOutputValueClass(Text.class);
	    
	    deduplication.setOutputKeyClass(Text.class);
	    deduplication.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(deduplication, new Path(args[5]));
	    FileOutputFormat.setOutputPath(deduplication, new Path(args[6]));
	    
	    
	    
	    
	    /*
		 * ******** TEXTUAL ENDS**********
		 * 
		 */
	    
	    
	    Job tdm1 =  Job.getInstance(getConf(),"tdm1");

		tdm1.setMapperClass(tdmMap1.class);
		tdm1.setReducerClass(tdmReducer1.class);
		
		tdm1.setOutputKeyClass(Text.class);
		tdm1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(tdm1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(tdm1, new Path(args[7]));
	    

	    
	    Job tdm2 =  Job.getInstance(getConf(),"tdm2");

		tdm2.setMapperClass(tdmMapper2.class);
		tdm2.setReducerClass(tdmReducer2.class);
		
		tdm2.setOutputKeyClass(Text.class);
		tdm2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(tdm2, new Path(args[7]));
	    FileOutputFormat.setOutputPath(tdm2, new Path(args[8]));
	    
	   
	    Job tdm3 =  Job.getInstance(getConf(),"tdm3");

		tdm3.setMapperClass(tdmMapper3.class);
		tdm3.setReducerClass(tdmReducer3.class);
		
		tdm3.setOutputKeyClass(Text.class);
		tdm3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(tdm3, new Path(args[7]));
	    FileOutputFormat.setOutputPath(tdm3, new Path(args[9]));

	    /*WAITFORCOMPLETION()
	     * DO MATRIX MULTIPLICATON HERE
	     * */
	    
	    Job matrixmultiplication1 =  Job.getInstance(getConf(),"matrixmultiplication1");

		matrixmultiplication1.setMapperClass(MatrixMultiplicationMap1.class);
		matrixmultiplication1.setReducerClass(MatrixMultiplicationReduce1.class);
		
		matrixmultiplication1.setOutputKeyClass(IntWritable.class);
		matrixmultiplication1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(matrixmultiplication1, new Path(args[10]));
	    FileOutputFormat.setOutputPath(matrixmultiplication1, new Path(args[11]));
	    

		Job matrixmultiplication2 =  Job.getInstance(getConf(),"matrixmultiplication2");

		matrixmultiplication2.setMapperClass(MatrixMultiplicationMap2.class);
		matrixmultiplication2.setReducerClass(MatrixMultiplicationReduce2.class);
		
		matrixmultiplication2.setOutputKeyClass(Text.class);
		matrixmultiplication2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(matrixmultiplication2, new Path(args[11]));
	    FileOutputFormat.setOutputPath(matrixmultiplication2, new Path(args[12]));
	    
	    Job conceptualfilter1 =  Job.getInstance(getConf(),"conceptualfilter1");

		conceptualfilter1.setMapperClass(ConceptualFilterMap1.class);

	    conceptualfilter1.setReducerClass(ConceptualFilterReduce1.class);
		
		conceptualfilter1.setOutputKeyClass(Text.class);
		conceptualfilter1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(conceptualfilter1, new Path(args[12]));
	    FileOutputFormat.setOutputPath(conceptualfilter1, new Path(args[13]));


	    Job conceptualfilter2 =  Job.getInstance(getConf(),"conceptualfilter2");

		conceptualfilter2.setMapperClass(ConceptualFilterMap2.class);

	    conceptualfilter2.setReducerClass(ConceptualFilterReduce2.class);
	    
	    conceptualfilter2.setMapOutputKeyClass(IntWritable.class);
	    conceptualfilter2.setMapOutputValueClass(Text.class);
		
		conceptualfilter2.setOutputKeyClass(Text.class);
		conceptualfilter2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(conceptualfilter2, new Path(args[13]));
	    FileOutputFormat.setOutputPath(conceptualfilter2, new Path(args[14]));

	    Job conceptualfilter3 =  Job.getInstance(getConf(),"conceptualfilter3");

		conceptualfilter3.setMapperClass(ConceptualFilterMap3.class);

	    conceptualfilter3.setReducerClass(ConceptualFilterReduce3.class);
	    
		conceptualfilter3.setOutputKeyClass(Text.class);
		conceptualfilter3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(conceptualfilter3, new Path(args[14]));
	    FileOutputFormat.setOutputPath(conceptualfilter3, new Path(args[15]));

	    ControlledJob controlledJob1 =  new ControlledJob(shinglecount, null);
	    ControlledJob controlledJob2 = new ControlledJob(docshavingterm,null);
	    ControlledJob controlledJob3 =  new ControlledJob(tfidf, null);
	    ControlledJob controlledJob4 =  new ControlledJob(shingletfidf, null);
	    ControlledJob controlledJob5 =  new ControlledJob(fingerprint, null);
	    ControlledJob controlledJob6 =  new ControlledJob(deduplication, null);
	    ControlledJob controlledJob7 =  new ControlledJob(tdm1, null);
	    ControlledJob controlledJob8 =  new ControlledJob(tdm2, null);
	    ControlledJob controlledJob9 =  new ControlledJob(tdm3, null);
	    ControlledJob controlledJob10 =  new ControlledJob(matrixmultiplication1, null);
	    ControlledJob controlledJob11 =  new ControlledJob(matrixmultiplication2, null);
	    ControlledJob controlledJob12 =  new ControlledJob(conceptualfilter1, null);
	    ControlledJob controlledJob13 =  new ControlledJob(conceptualfilter2, null);
	    ControlledJob controlledJob14 =  new ControlledJob(conceptualfilter3, null);
	    
	    
	    JobControl control =  new JobControl("Simhash");
	    
	    control.addJob(controlledJob1);
	    control.addJob(controlledJob2);
	    control.addJob(controlledJob3);
	    control.addJob(controlledJob4);
	    control.addJob(controlledJob5);
	    control.addJob(controlledJob6);
	    control.addJob(controlledJob7);
	    control.addJob(controlledJob8);
	    control.addJob(controlledJob9);
	    
	    controlledJob2.addDependingJob(controlledJob1);
	    controlledJob3.addDependingJob(controlledJob2);
	    controlledJob4.addDependingJob(controlledJob3);
	    controlledJob5.addDependingJob(controlledJob4);
	    controlledJob6.addDependingJob(controlledJob5);
	    controlledJob8.addDependingJob(controlledJob7);
	    controlledJob9.addDependingJob(controlledJob8);
		
	    
	    Thread thread =  new Thread(control);
	    thread.setDaemon(true);
	    thread.start();
	    
	    while(true)
	    {
	    	
	    	if (control.allFinished())
		    {
	    		System.out.println("\n\n\n\n\nExecuting static code...\n\n\n\n");
	    	    MatrixImplementor mi = new MatrixImplementor();
	        	DenseMatrix tdm = mi.buildmatrix("/home/sahil/tdm3", matrixmultiplication1.getConfiguration());
	        	SingularValueDecomposition svd = mi.getsvd(tdm);
	        	
	        	Matrix U = svd.getU();
	        	Matrix S = svd.getS();
	        	Matrix V = svd.getV();
	        	
	        	Matrix Vtranspose = V.transpose();
	        	int columnsS = S.columnSize(); //dont know exactly which matrices to multiply
	        	int rowsS = S.rowSize();
	        	
	        	mi.writetofile(S, new Path("/home/sahil/svd1/matrix1"), matrixmultiplication1.getConfiguration());
	        	mi.writetofile(Vtranspose, new Path("/home/sahil/svd1/matrix2"), matrixmultiplication1.getConfiguration());
	        	
	    		control.addJob(controlledJob10);
	    	    control.addJob(controlledJob11);
	    	    
	    	    
	    	    controlledJob10.addDependingJob(controlledJob9);
	    	    controlledJob11.addDependingJob(controlledJob10);
	    	    
	    	    while(true)
	    	    {
	    	    	if(control.allFinished())
	    	    		{
	    	        	System.out.println("now writing this : "+ mi.docmap.toString());
	    	        	
	    	    		String json = JsonWriter.objectToJson(mi.docmap);
	    	    		conceptualfilter1.getConfiguration().set("map", json);
	    	    		control.addJob(controlledJob12);
	    	    		control.addJob(controlledJob13);
	    	    		control.addJob(controlledJob14);
	    	    		
	    	    		controlledJob12.addDependingJob(controlledJob11);
	    	    		controlledJob13.addDependingJob(controlledJob12);
	    	    		controlledJob14.addDependingJob(controlledJob13);
	    	    		
	    	    		while(true)
	    	    		{
	    	    			if(control.allFinished())
	    	    				break;
	    	    		}
	    	    		
	    	    		break;
	    	    		}
	    	    }
	    		control.stop();
	    		break;
			}
	    }


//		
	    
	    
	    return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		if(args.length !=16)
		{
			System.out.println("the arguments are too many or too less");
			System.exit(1);
		}
		else
		{
			ToolRunner.run(new Simhash(), args);
		}
	}
	
	@Override
	public Configuration getConf() 
	{
		return new Configuration();
	}
	@Override
	public void setConf(Configuration arg0) 
	{
		// TODO Auto-generated method stub
		
	}
	
	

}
