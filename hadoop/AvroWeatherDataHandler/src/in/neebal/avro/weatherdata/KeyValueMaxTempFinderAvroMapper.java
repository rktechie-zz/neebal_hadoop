package in.neebal.avro.weatherdata;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyValueMaxTempFinderAvroMapper extends
		Mapper<AvroKey<Integer>, AvroValue<WeatherData>, IntWritable, IntWritable> {

	IntWritable key = new IntWritable();
	IntWritable value = new IntWritable();
	
	public void map(AvroKey<Integer> ikey, AvroValue<WeatherData> ivalue, Context context)
			throws IOException, InterruptedException {
		key.set(ikey.datum());
		value.set(ivalue.datum().getTemperature());
		
		context.write(key, value);

	}

}
