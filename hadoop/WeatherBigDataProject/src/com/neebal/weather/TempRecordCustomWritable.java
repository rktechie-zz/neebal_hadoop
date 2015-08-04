package com.neebal.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TempRecordCustomWritable implements Writable {

	private IntWritable stationNo;
	private IntWritable temperature;
	
	public TempRecordCustomWritable(){
		stationNo = new IntWritable();
		temperature = new IntWritable();
	}
	
	public IntWritable getStationNo() {
		return stationNo;
	}

	public void setStationNo(IntWritable stationNo) {
		this.stationNo = stationNo;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(IntWritable temperature) {
		this.temperature = temperature;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		stationNo.readFields(input);
		temperature.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		stationNo.write(output);
		temperature.write(output);
	}
	
	public static TempRecordCustomWritable createInstance(int stationNo, int temperature){
		TempRecordCustomWritable r = new TempRecordCustomWritable();
		r.setStationNo(new IntWritable(stationNo));
		r.setTemperature(new IntWritable(temperature));
		
		return r;
	}

}
