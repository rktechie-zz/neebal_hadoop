package com.neebal.library.wordindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordIndexDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordIndexer");
		job.setJarByClass(com.neebal.library.wordindex.WordIndexDriver.class);
		job.setMapperClass(com.neebal.library.wordindex.WordIndexMapper.class);

		job.setReducerClass(com.neebal.library.wordindex.WordIndexReducer.class);
		job.setCombinerClass(com.neebal.library.wordindex.WordIndexCombiner.class);
		
		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/root/Documents/Rishabh/librarian_input_files"));
		FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/librarian_output_files"));

		if (!job.waitForCompletion(true))
			return;
	}

}
