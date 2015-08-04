package com.neebal.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.neebal.weathersensor.json.JsonInputFormat;

public class MaxTempFinderJsonDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "MaxTempFinder");
		job.setJarByClass(com.neebal.weather.MaxTempFinderJsonDriver.class);
		job.setMapperClass(com.neebal.weather.MaxTempFinderJsonMapper.class);

		job.setReducerClass(com.neebal.weather.MaxTempFinderReducer.class);
		job.setCombinerClass(MaxTempFinderReducer.class);
		
		job.setInputFormatClass(JsonInputFormat.class); // used for xml input
		
		// TODO: specify output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/root/Documents/Rishabh/weather_data_json_input/"));
		FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/weather_sensor_json_output/hadoop_max_temp_year"));

		if (!job.waitForCompletion(true))
			return;
	}

}
