package com.neebal.weather;

import in.neebal.temp.writables.TempRecordWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceMaxTempFinderMapper extends
		Mapper<Text, TempRecordWritable, IntWritable, TempRecordCustomWritable> {

	public void map(Text ikey, TempRecordWritable ivalue, Context context)
			throws IOException, InterruptedException {
		context.write(new IntWritable(Integer.parseInt(ikey.toString())),
				
				TempRecordCustomWritable.createInstance(Integer.parseInt(ivalue.getStation().toString()),
						ivalue.getTemperature().get()));
	}

}
