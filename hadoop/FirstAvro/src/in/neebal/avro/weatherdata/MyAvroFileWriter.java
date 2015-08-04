package in.neebal.avro.weatherdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class MyAvroFileWriter {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		DatumWriter<WeatherData> datumWriter = 
				new SpecificDatumWriter<WeatherData>(WeatherData.class);
		
		DataFileWriter<WeatherData> writer = 
				new DataFileWriter<WeatherData>(datumWriter);

		//For compression
		writer.setCodec(CodecFactory.snappyCodec());
		
		writer.create(WeatherData.SCHEMA$, new File("/root/Documents/Rishabh/avrofiles/weather_data_2.avro"));
		
		File folder = new File("/root/Documents/Rishabh/weather_data_input_new");
		File[] files = folder.listFiles();
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
					
					writer.append(weatherData);
				}
				line = br.readLine();
			}
			br.close();
		}
		writer.close();
	}

}
