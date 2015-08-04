package in.neebal.temp.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TempRecordWritable implements Writable {

	private Text station = new Text();
	private IntWritable temperature = new IntWritable();
	private IntWritable hours = new IntWritable();
	private IntWritable day = new IntWritable();
	
	public TempRecordWritable(){
		station = new Text();
		temperature = new IntWritable();
		hours = new IntWritable();
		day = new IntWritable();
	}
	
	public Text getStation() {
		return station;
	}

	public void setStation(Text station) {
		this.station = station;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(IntWritable temperature) {
		this.temperature = temperature;
	}

	public IntWritable getHours() {
		return hours;
	}

	public void setHours(IntWritable hours) {
		this.hours = hours;
	}

	public IntWritable getDay() {
		return day;
	}

	public void setDay(IntWritable day) {
		this.day = day;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		station.readFields(input);
		temperature.readFields(input);
		hours.readFields(input);
		day.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		station.write(output);
		temperature.write(output);
		hours.write(output);
		day.write(output);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
