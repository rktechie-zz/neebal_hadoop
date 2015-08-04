package in.neebal.avro.weatherdata;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class MyAvroFileReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DatumReader<WeatherData> reader = 
				new SpecificDatumReader<WeatherData>(WeatherData.class);
		
		DataFileReader<WeatherData> fileReader = 
				new DataFileReader<>(
						new File("/root/Documents/Rishabh/avrofiles/weather_data.avro"), 
						reader);
		while(fileReader.hasNext()){
			WeatherData weatherData = fileReader.next();
			
			//We use ToStringBuilder for printing of weatherData object.
			//We could have also used the get function to print it.
			System.out.println(ToStringBuilder.reflectionToString(weatherData, ToStringStyle.SIMPLE_STYLE));
		}
		fileReader.close();
	}

}
