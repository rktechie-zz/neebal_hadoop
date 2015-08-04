weather_records = load '/root/Documents/Rishabh/weather_data_json_input'
	using JsonLoader('stationNo:int,year:int,day:int,month:int,hours:int,minutes:int,temp:int');
filtered_records = filter weather_records by temp is not null;
projected_records = foreach filtered_records generate year,temp;
grpd_records = group projected_records by year;
max_temp_year = foreach grpd_records generate group as year, MAX(projected_records.temp);
store max_temp_year into '/root/Documents/Rishabh/pig/weather_data/output' using PigStorage(',');