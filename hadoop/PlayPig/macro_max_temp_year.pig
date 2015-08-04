import '/root/Documents/Rishabh/hadoop/PlayPig/max_in_group.macro';

weather_records = load '/root/Documents/Rishabh/weather_data_json_input'
	using JsonLoader('stationNo:int,year:int,day:int,month:int,hours:int,minutes:int,temp:int');
filtered_records = filter weather_records by temp is not null;
projected_records = foreach filtered_records generate year,temp;

--using the macro
max_temp_year = max_in_group(projected_records, year, temp);

store max_temp_year into '/root/Documents/Rishabh/pig/weather_data/output' using PigStorage(',');