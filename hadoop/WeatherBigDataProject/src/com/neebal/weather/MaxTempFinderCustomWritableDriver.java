package com.neebal.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempFinderCustomWritableDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "MaxTempFinder");
		job.setJarByClass(com.neebal.weather.MaxTempFinderCustomWritableDriver.class);
		job.setMapperClass(com.neebal.weather.MaxTempFinderCustomWritableMapper.class);

		job.setReducerClass(com.neebal.weather.MaxTempFinderCustomWritableReducer.class);
		
		// TODO: specify output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TempRecordCustomWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/root/Documents/Rishabh/weather_data_input"));
		FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/weather_sensor_output/hadoop_max_temp_year"));

		if (!job.waitForCompletion(true))
			return;
	}

}
