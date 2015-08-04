weather_records = load '/root/Documents/Rishabh/weather_data_input'
	using PigStorage('|')
	as (station,year:int,daymonth,hourminute,temperature:int);
filtered_records = filter weather_records by temperature is not null;
projected_records = foreach filtered_records generate year,temperature;
grpd_records = group projected_records by year;
max_temp_year = foreach grpd_records generate group as year, MAX(projected_records.temperature);
store max_temp_year into '/root/Documents/Rishabh/pig/weather_data/output' using PigStorage(',');