./batchInsert ../test_data/5-10000a.txt testdb test1 5 false
./batchInsert ../test_data/5-10000a.txt testdb test1 5 true


./query "testdb" "test1" "[A,B,C,D]" "{(A = Delaware) AND (B = Colombia) OR ((C = 2) OR (C=5))}" "12" "FILESCAN"


./query "testdb" "test1" "[A,B,C,D]" "{(A = Delaware) AND (B = Colombia) OR ((C = 2) OR (C=5))}" "12" "COLUMNSCAN"

./query "testdb" "test1" "[A,B,C,D]" "{(A = Washington) OR (B = Washington}" "20" "FILESCAN"

./index "testdb" "test1" "A" "BTREE"
./index "testdb" "test1" "B" "BTREE"
./index "testdb" "test1" "C" "BTREE"

./index "testdb" "test1" "A" "BITMAP"
./index "testdb" "test1" "B" "BITMAP"
./index "testdb" "test1" "C" "BITMAP"

./index "testdb" "test1" "A" "CBITMAP"
./index "testdb" "test1" "B" "CBITMAP"
./index "testdb" "test1" "C" "CBITMAP"

./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "FILESCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "COLUMNSCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "CBITMAP"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "BITMAP"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B < Indiana)}" "15" "BTREE"

./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B = Tuvalu)}" "15" "BITMAP"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) AND ((B = Tuvalu)}" "15" "CBITMAP"

./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) OR ((B < Indiana)}" "15" "FILESCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) OR ((B < Indiana)}" "15" "COLUMNSCAN"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) OR ((B < Indiana)}" "15" "CBITMAP"
./query "testdb" "test1" "[A,B,C,D]" "{(A > 0)}" "15" "BITMAP"
./query "testdb" "test1" "[A,B,C,D]" "{(A > Arizona) OR ((B < Indiana)}" "15" "BTREE"

./query "testdb" "test1" "[A,B,C,D]" "{(A = Wyoming)}" "15" "BTREE"

./query "testdb" "test1" "[A,B,C,D]" "{(A = Wyoming}}" "15" "FILESCAN"
./delete "testdb" "test1" "{(A = Texas)}" "15" "false"
./delete "testdb" "test1" "{(B = Wyoming)}" "15" "true"

For tuple scan we need a minimum buf size of 11, this depends on num of column a table has, 
as all scans open together and close together we need to have some amount of minimum buffer.

