package com.neebal.weather;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempFinderJsonMapper extends
		Mapper<LongWritable, MapWritable, IntWritable, IntWritable> {

	private IntWritable key = new IntWritable();
	private IntWritable value = new IntWritable();
	
	public void map(LongWritable ikey, MapWritable ivalue, Context context)
			throws IOException, InterruptedException {
		for(Entry<Writable, Writable> entry : ivalue.entrySet()){
			String entryKey = entry.getKey().toString();
			if(entryKey.equals("year")){
				key.set(((IntWritable)entry.getValue()).get());
			}
			else if(entryKey.equals("temp")){
				value.set(((IntWritable)entry.getValue()).get());
			}
		}
		context.write(key, value);
	}

}
