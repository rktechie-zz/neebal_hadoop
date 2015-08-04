package in.neebal.dbanalysis.mapred;

import in.neebal.dbanalysis.writables.StudentRecordWritable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentCollegeAddressMapper extends
		Mapper<LongWritable, StudentRecordWritable, Text, Text> {

	private Text key = new Text();
	private Text value = new Text();
	
	public void map(LongWritable ikey, StudentRecordWritable ivalue, Context context)
			throws IOException, InterruptedException {
		String collAddress = ivalue.getCollegeAddress().toString();
		String[] tokens = collAddress.split(" ");
		if(tokens.length == 1){
			key.set(ivalue.getStudentName().toString());
			value.set(collAddress);
			
			context.write(key, value);
		}
	}

}
