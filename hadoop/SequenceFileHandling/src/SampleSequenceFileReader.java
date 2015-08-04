import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class SampleSequenceFileReader {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration configuration = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(configuration, 
				SequenceFile.Reader.file(
						new Path("/root/Documents/Rishabh/hadoop/sequence_file_output/sample.seq")
						));
		Text areaKey = new Text();
		IntWritable pincodeValue = new IntWritable();
		while(reader.next(areaKey, pincodeValue)){
			System.out.println(areaKey.toString() + "\n" + pincodeValue.get());
		}
		
		reader.close();
	}

}
