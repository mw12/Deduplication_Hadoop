import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class StringArrayWritable implements Writable{
	public ArrayWritable array;
	public StringArrayWritable()
	{
		this(Collections.EMPTY_LIST);
	}

	public StringArrayWritable(List<Text> texts) {
		array = new ArrayWritable(Text.class, texts.toArray(new Writable[texts.size()]));
	}

	public void readFields(DataInput in) throws IOException {
		array.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		array.write(out);
	}

}
