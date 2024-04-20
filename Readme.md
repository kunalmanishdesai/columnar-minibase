./batchInsert ../test_data/5-10000a.txt testdb test1 5 false
./batchInsert ../test_data/5-10000a.txt testdb test1 5 true

./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "COLUMNSCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A = Delaware) AND (B = Colombia) OR ((C = 2) OR (C=5))}" "12" "FILESCAN"

./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "COLUMNSCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A = Delaware) AND (B = Colombia) OR ((C = 2) OR (C=5))}" "12" "COLUMNSCAN"

./query "testdb" "test1" "[A,B,C,D]" "{(A = Washington) OR (B = Washington}" "300" "FILESCAN"

./index "testdb" "test1" "C" "BTREE"


For tuple scan we need a minimum buf size of 11, this depends on num of column a table has, 
as all scans open together and close together we need to have some amount of minimum buffer.

