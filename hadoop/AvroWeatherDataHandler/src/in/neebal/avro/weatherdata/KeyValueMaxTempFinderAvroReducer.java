package in.neebal.avro.weatherdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyValueMaxTempFinderAvroReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<IntWritable> iterator = values.iterator();
		int maxTemp = 0;
		if(iterator.hasNext()){
			maxTemp = iterator.next().get();
		}
		
		while(iterator.hasNext()){
			int temp = iterator.next().get();
			if(temp > maxTemp){
				maxTemp = temp;
			}
		}
		
		context.write(key, new IntWritable(maxTemp));
	}

}
