package com.neebal.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SequenceMaxTempFinderDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(com.neebal.weather.SequenceMaxTempFinderDriver.class);
		job.setMapperClass(com.neebal.weather.SequenceMaxTempFinderMapper.class);

		job.setReducerClass(com.neebal.weather.MaxTempFinderCustomWritableReducer.class);

		// TODO: specify output types
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TempRecordCustomWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		SequenceFileInputFormat.setInputPaths(job, "/root/Documents/Rishabh/sequence_file_output");
		FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/sequence_file_hadoop_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
