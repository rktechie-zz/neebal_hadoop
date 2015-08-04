import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class SampleSequenceFileWriter {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration configuration = new Configuration();
		 SequenceFile.Writer writer = SequenceFile.createWriter(configuration, 
				SequenceFile.Writer.file(new Path("/root/Documents/Rishabh/hadoop/sequence_file_output/sample.seq")), 
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(IntWritable.class));
		
		writer.append(new Text("Andheri station west"), new IntWritable(400001));
		writer.append(new Text("Four Bungalows"), new IntWritable(400053));
		writer.close();
	}

}
