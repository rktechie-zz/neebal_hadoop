package com.neebal.weather;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempFinderXMLMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable key = new IntWritable();
	private IntWritable value = new IntWritable();
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String xmlData = ivalue.toString();
		XMLStreamReader reader = null;
		try {
			reader = XMLInputFactory.newInstance().createXMLStreamReader(
					new ByteArrayInputStream(xmlData.getBytes()));
			String currentElement = "";
			while(reader.hasNext()){
				int code = reader.next();
				switch(code){
				case XMLStreamConstants.START_ELEMENT:
					currentElement = reader.getLocalName();
					break;
				case XMLStreamConstants.CHARACTERS:
					String text = reader.getText().trim();
					if(currentElement.equals("year")){
						if(text != null && !text.isEmpty()){
							key.set(Integer.parseInt(text));
						}
					}
					else if(currentElement.equals("temp")){
						if(text != null && !text.isEmpty()){
							value.set(Integer.parseInt(text));
						}
					}
					break;
				default:
					break;
				}
			}
			context.write(key, value);
		} catch (XMLStreamException | FactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(reader != null){
				try{
					reader.close();	
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}

}
