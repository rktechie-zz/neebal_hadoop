package weather;

import in.neebal.temp.writables.TempRecordWritable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SampleWeatherSeqFileWriter {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration configuration = new Configuration();
		 SequenceFile.Writer writer = SequenceFile.createWriter(configuration, 
				SequenceFile.Writer.file(new Path("/root/Documents/Rishabh/sequence_file_output/weather_sample_2.seq")), 
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(TempRecordWritable.class));
		
		String inputDir = "/root/Documents/Rishabh/weather_data_input_new";

		File inputFileDir = new File(inputDir);
		File[] inputFiles = inputFileDir.listFiles();
		BufferedReader br = null;
		String line = null;

		for (File file : inputFiles) {
			br = new BufferedReader(new FileReader(file));
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\|");
				if (tokens.length == 5) {
					String year = tokens[1];
					String station = tokens[2];
					int temp = Integer.parseInt(tokens[4]);
					String temphours = tokens[3].substring(0, 2);
					int hours = Integer.parseInt(temphours);
					int day = Integer.parseInt(tokens[2].substring(0,2));
					TempRecordWritable tempRecordWritable = new TempRecordWritable();
					tempRecordWritable.setStation(new Text(station));
					tempRecordWritable.setTemperature(new IntWritable(temp));
					tempRecordWritable.setHours(new IntWritable(hours));
					tempRecordWritable.setDay(new IntWritable(day));
					
					writer.append(new Text(year), tempRecordWritable);
				}
				line = br.readLine();
			}
		}
		br.close();
		writer.close();
	}

}
