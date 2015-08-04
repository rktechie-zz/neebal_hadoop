student_records =  load '/user/rishabh/student_input/students_1'
	as (id:chararray,name:chararray,contact:chararray);

grouped_records = group student_records by (id,name);
projected_records = foreach grouped_records generate group.id as id, group.name as name, BagToString(student_records.contact, ',') as contacts;



student_marks_records =  load '/user/rishabh/student_input/students_2'
	as (id:chararray,sem:chararray,marks:int);
	
marks_records = foreach student_marks_records generate id as id, CONCAT(sem,':',(chararray)marks) as sem_marks;
grouped_marks_records = group marks_records by (id);
projected_marks_records = foreach grouped_marks_records generate group as id, BagToString(marks_records.sem_marks, ' ') as sem_marks;



final_record = COGROUP projected_records by id, projected_marks_records by id;
grouped_final_record = foreach final_record generate group as id, BagToString(projected_records.name), BagToString(projected_records.contacts), BagToString(projected_marks_records.sem_marks);