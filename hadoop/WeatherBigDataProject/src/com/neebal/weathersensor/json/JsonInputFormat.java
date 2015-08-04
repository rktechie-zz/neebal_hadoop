/**
 * 
 */
package com.neebal.weathersensor.json;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * @author root
 *
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, MapWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
				return new MyJsonReader();
	}
	
	private static class MyJsonReader extends RecordReader<LongWritable, MapWritable>{

		private LineRecordReader reader = new LineRecordReader();
		private MapWritable writable = new MapWritable();
		private JsonParser parser = new JsonParser();
		
		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public MapWritable getCurrentValue() throws IOException,
				InterruptedException {
			return writable;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			reader.initialize(arg0, arg1);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean b = reader.nextKeyValue();
			if(b == false){
				return b;
			}
			String line = reader.getCurrentValue().toString();
			 
			 writable.clear();
			 JsonObject obj = (JsonObject)parser.parse(line);
			 Set<Entry<String, JsonElement>> entries = obj.entrySet();
			 for(Entry<String, JsonElement> entry: entries){
				 JsonElement value = entry.getValue();
				 IntWritable valueInt = new IntWritable(value.getAsInt());
				 writable.put(new Text(entry.getKey()), valueInt);
			 }
			 return true;
		}
		
	}

}
