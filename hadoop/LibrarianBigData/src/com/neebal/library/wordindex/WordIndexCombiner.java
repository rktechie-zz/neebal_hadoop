package com.neebal.library.wordindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordIndexCombiner extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> documentIds = new ArrayList<>();
		
		String documentId = null;
		for(Text value: values){
			documentId = value.toString();
			if(!documentIds.contains(documentId)){
				documentIds.add(documentId);
				context.write(_key, value);
			}
		}
	}

}
