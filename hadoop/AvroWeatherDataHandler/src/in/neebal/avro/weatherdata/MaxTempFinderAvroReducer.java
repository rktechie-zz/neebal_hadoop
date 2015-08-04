package in.neebal.avro.weatherdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;

public class MaxTempFinderAvroReducer extends AvroReducer<Integer, WeatherData, MaxTempYear> {

	@Override
	public void reduce(Integer yearKey, Iterable<WeatherData> value,
			AvroCollector<MaxTempYear> collector, Reporter reporter) throws IOException {
		Iterator<WeatherData> iterator= value.iterator();
		int maxTemp = 0;
		int hours = 0;
		if(iterator.hasNext()){
			maxTemp = iterator.next().getTemperature();
			hours = iterator.next().getHours();
		}
		while(iterator.hasNext()){
			WeatherData weatherData = iterator.next();
			int temp = weatherData.getTemperature();
			int temp_hours = weatherData.getHours();
			if(temp > maxTemp){
				maxTemp = temp;
				hours = temp_hours;
			}
		}
		
		MaxTempYear maxTempYear = new MaxTempYear(yearKey, maxTemp, hours);
		collector.collect(maxTempYear);
	}

}
