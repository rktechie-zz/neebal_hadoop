student_records =  load '/user/rishabh/student_input/students_1'
	as (id:chararray,name:chararray,contact:chararray);

grouped_records = group student_records by (id,name);
projected_records = foreach grouped_records generate group.id as id, group.name as name, BagToString(student_records.contact, ',') as contacts;
store projected_records into '/user/rishabh/student_output';