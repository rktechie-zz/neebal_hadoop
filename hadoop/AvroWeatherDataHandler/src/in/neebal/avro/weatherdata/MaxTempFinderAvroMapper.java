package in.neebal.avro.weatherdata;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;

public class MaxTempFinderAvroMapper extends AvroMapper<WeatherData, Pair<Integer, WeatherData>> {
	@Override
	public void map(WeatherData datum,
			AvroCollector<Pair<Integer, WeatherData>> collector,
			Reporter reporter) throws IOException {
		
		Pair<Integer, WeatherData> pair = new Pair<Integer, WeatherData>(datum.getYear(), datum);
		collector.collect(pair);
		
	}
}
