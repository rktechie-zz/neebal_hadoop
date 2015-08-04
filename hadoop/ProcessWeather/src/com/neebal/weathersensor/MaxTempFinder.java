package com.neebal.weathersensor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class MaxTempFinder {
	public static void main(String arg[]) throws IOException {
		String inputDir = "/root/Documents/Rishabh/hadoop_required_files_neebal/weather_data_input";
		String outputDir = "/root/Documents/Rishabh/weather_sensor_output/max_temp_year";

		File inputFileDir = new File(inputDir);
		File[] inputFiles = inputFileDir.listFiles();
		BufferedReader br = null;
		String line = null;
		HashMap<String, Integer> maxTempMap = new HashMap<>();

		for (File file : inputFiles) {
			br = new BufferedReader(new FileReader(file));
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\|");
				if (tokens.length == 5) {
					String year = tokens[1];
					int temp = Integer.parseInt(tokens[4]);

					Integer tempFound = maxTempMap.get(year);
					if (tempFound == null) {
						maxTempMap.put(year, temp);
					} else {
						if (tempFound.intValue() < temp) {
							maxTempMap.put(year, temp);
						}
					}
				}
				line = br.readLine();
			}
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir));
		for (Entry<String, Integer> yearTemp : maxTempMap.entrySet()) {
			writer.write(yearTemp.getKey() + " " + yearTemp.getValue());
			writer.newLine();
		}
		writer.close();
		br.close();
	}
}
