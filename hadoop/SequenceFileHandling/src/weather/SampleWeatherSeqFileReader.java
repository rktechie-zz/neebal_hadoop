package weather;

import in.neebal.temp.writables.TempRecordWritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SampleWeatherSeqFileReader {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration configuration = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(configuration, 
				SequenceFile.Reader.file(
						new Path("/root/Documents/Rishabh/hadoop/sequence_file_output/weather_sample.seq")
						));
		Text year = new Text();
		TempRecordWritable tempRecordWritable = new TempRecordWritable();
		while(reader.next(year, tempRecordWritable)){
			System.out.println("Year: " + year.toString() + "\n" + "Station: " + tempRecordWritable.getStation().toString() + "\n" + "Temperature: " + tempRecordWritable.getTemperature().get() + "\n" + "Hours: " + tempRecordWritable.getHours().get());
		}
		
		reader.close();
	}

}
