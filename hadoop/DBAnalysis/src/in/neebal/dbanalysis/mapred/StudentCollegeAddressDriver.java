package in.neebal.dbanalysis.mapred;

import in.neebal.dbanalysis.writables.StudentRecordWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentCollegeAddressDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306/learning", "root", "");
		
		Job job = Job.getInstance(conf, "StudentCollegeAddress");
		job.setJarByClass(in.neebal.dbanalysis.mapred.StudentCollegeAddressDriver.class);
		job.setMapperClass(in.neebal.dbanalysis.mapred.StudentCollegeAddressMapper.class);

		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(DBInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		DBInputFormat.setInput(job, StudentRecordWritable.class,
				"students", null, null, "id", "student_name", "college_address");
		FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/hadoop/learning_db_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
