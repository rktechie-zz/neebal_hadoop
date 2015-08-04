package com.neebal.library.wordindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text documentIdValue = new Text(); 
	private Text key = new Text();
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String name = ((FileSplit)context.getInputSplit()).getPath().getName();
		documentIdValue.set(name);
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		List<String> keysForLineEncountered = new ArrayList<>();
		String line = ivalue.toString();
		String[] tokens = StringUtils.split(line);
		for(String token: tokens){
			if(!keysForLineEncountered.contains(token)){
				keysForLineEncountered.add(token);
				key.set(token);
				context.write(key, documentIdValue);
			}
		}
	}

}
