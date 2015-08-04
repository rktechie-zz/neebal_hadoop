package com.neebal.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempFinderCustomWritableMapper extends Mapper<LongWritable, Text, IntWritable, TempRecordCustomWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] tokens = line.split("\\|");
		if(tokens.length == 5){
			int year = Integer.parseInt(tokens[1]);
			int temp = Integer.parseInt(tokens[4]);
			int stationNo = Integer.parseInt(tokens[0]);
			
			context.write(new IntWritable(year), TempRecordCustomWritable.createInstance(stationNo, temp));
		}

	}

}
