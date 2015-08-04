package in.neebal.avro.weatherdata;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyValueMaxTempFinderAvroDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KeyValueMaxTempFinderAvro");
		job.setJarByClass(in.neebal.avro.weatherdata.KeyValueMaxTempFinderAvroDriver.class);
		job.setMapperClass(in.neebal.avro.weatherdata.KeyValueMaxTempFinderAvroMapper.class);

		job.setReducerClass(in.neebal.avro.weatherdata.KeyValueMaxTempFinderAvroReducer.class);

		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
		AvroJob.setInputValueSchema(job, WeatherData.SCHEMA$);
		
		// TODO: specify output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/user/rishabh/weather_data_input/avro_keyvalue_files"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/user/rishabh/weather_data_output/avro_keyvalue_output"));
		//FileInputFormat.setInputPaths(job, new Path("/root/Documents/Rishabh/avrofiles/key_value"));
		//FileOutputFormat.setOutputPath(job, new Path("/root/Documents/Rishabh/avro_keyvalue_output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
