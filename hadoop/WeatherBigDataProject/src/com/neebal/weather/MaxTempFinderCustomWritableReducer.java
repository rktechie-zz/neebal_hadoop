package com.neebal.weather;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempFinderCustomWritableReducer extends Reducer<IntWritable, TempRecordCustomWritable, IntWritable, Text> {

	private Text value = new Text();
	
	public void reduce(IntWritable year, Iterable<TempRecordCustomWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<TempRecordCustomWritable> iterator = values.iterator();
		int maxTemp = 0;
		int stationNoOfMaxTemp = 0;
		TempRecordCustomWritable tempRecordCustomWritable = null;
		if(iterator.hasNext()){
			tempRecordCustomWritable = iterator.next();
			maxTemp = tempRecordCustomWritable.getTemperature().get();
			stationNoOfMaxTemp = tempRecordCustomWritable.getStationNo().get();
		}
		
		while(iterator.hasNext()){
			tempRecordCustomWritable = iterator.next();
			if(tempRecordCustomWritable.getTemperature().get() > maxTemp){
				maxTemp = tempRecordCustomWritable.getTemperature().get();
				stationNoOfMaxTemp = tempRecordCustomWritable.getStationNo().get();
			}
		}
		
		String value = StringUtils.join(new String[]{stationNoOfMaxTemp+"", maxTemp+""}, ',');
		this.value.set(value);
		context.write(year, this.value);
	}

}
