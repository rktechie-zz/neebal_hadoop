-- Mapping of json data types to pig data types
-- A json object is mapped to a tuple
-- An array of json objects is converted to a bag (a bag contains multiple tuples)

books = load '/root/Documents/Rishabh/books_input' 
	using JsonLoader('title:chararray, price:double, pages:int, authors :{(name:chararray, rating:int)}, pub:(name:chararray)');
	
store books into '/root/Documents/Rishabh/pig/books/output' using PigStorage(',');