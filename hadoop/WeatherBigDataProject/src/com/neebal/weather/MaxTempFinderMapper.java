package com.neebal.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempFinderMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] tokens = line.split("\\|");
		if(tokens.length == 5){
			int year = Integer.parseInt(tokens[1]);
			int temp = Integer.parseInt(tokens[4]);
			
			context.write(new IntWritable(year), new IntWritable(temp));
		}

	}

}
