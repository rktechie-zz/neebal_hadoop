import '/root/Documents/Rishabh/hadoop/PlayPig/max_in_group.macro';

weather_records = load '$input_path'
	using PigStorage('|')
	as (station,year:int,daymonth,hourminute,temp:int);
	
filtered_records = filter weather_records by temp is not null;
projected_records = foreach filtered_records generate year,temp;

--using the macro
max_temp_year = max_in_group(projected_records, year, temp);

store max_temp_year into '$output_path' 
	using PigStorage(',');
	
-- To run this pig file (param in the command line), type this command:
-- pig -f '/root/Documents/Rishabh/hadoop/PlayPig/parameter_max_temp_year.pig' -param input_path=/user/rishabh/weather_data_input/pipe_delimited_files -param output_path=/user/rishabh/weather_data_output/pipe_delimited_files_output


-- To run this pig file (param from another file i.e max_temp_year_parameters), type this command:
-- pig -f '/root/Documents/Rishabh/hadoop/PlayPig/parameter_max_temp_year.pig' -param_file '/root/Documents/Rishabh/hadoop/PlayPig/max_temp_year_parameters'