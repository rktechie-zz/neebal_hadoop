word_records = load '/root/Documents/Rishabh/word_count_input'
	as (word:chararray);
	
tokens = foreach word_records generate flatten(TOKENIZE(word));
-- tokens is a bag of single field tuples
-- eg: (cat)
--     (lat)
--     (cat)

grpd_records = group tokens by $0;
-- grpd_records is a bag of tuples where every tuple has the 1st field called group and 2nd field is called tokens which is a bag of single field tuples
-- eg: { 
--		 (cat, {(cat), (cat)})
--	     (lat, {(lat)}) 
--     }

word_count = foreach grpd_records generate group as word, COUNT(tokens) as frequency;