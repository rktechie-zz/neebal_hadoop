package in.neebal.avro.weatherdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumWriter;

public class MyAvroKeyValueFileWriter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), WeatherData.SCHEMA$);
		
		//a GenericRecord consisting of key (year), value (WeatherData) will be written in the file
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
		
		DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
		
		//set compression
		fileWriter.setCodec(CodecFactory.snappyCodec());
		
		fileWriter.create(keyValueSchema, 
				new File("/root/Documents/Rishabh/avrofiles/key_value/weather_data_key_value.avro"));
		
		File folder = new File("/root/Documents/Rishabh/weather_data_input");
		File[] files = folder.listFiles();
		//reading from the input txt files and appending in the avro file
		for(File file: files){
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while(line != null){
				String[] tokens = line.split("\\|");
				if(tokens.length == 5){
					WeatherData weatherData = WeatherData.newBuilder()
							.setStationNo(tokens[0])
							.setTemperature(Integer.parseInt(tokens[4]))
							.setYear(Integer.parseInt(tokens[1]))
							.setHours(Integer.parseInt(tokens[3].substring(0, 2)))
							.build();
					
					AvroKeyValue<Integer, WeatherData> genericRecord = new AvroKeyValue<>(
							new GenericData.Record(keyValueSchema));
					genericRecord.setKey(weatherData.getYear());
					genericRecord.setValue(weatherData);
					
					fileWriter.append(genericRecord.get());
				}
				line = br.readLine();
			}
			br.close();
		}
		fileWriter.close();
	}

}
