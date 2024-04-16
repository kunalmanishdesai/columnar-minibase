package commands;

import columnar.TupleScan;
import columnar.Utils;
import heap.Tuple;

public class FileScan extends ScanCommand {

    private final TupleScan tupleScan;
    public FileScan(String inputCommand) {

        super(inputCommand);
        tupleScan = new TupleScan(columnarFile,outputTupleAttributes);
    }

    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = tupleScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(count,tuple,attrTypes));
            count++;
        }

        tupleScan.closeScan();
        System.out.println("Number of tuples printed: " + count);
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {

//        String test = "testdb test1 [A,B,C,D,E] {(A = Washington)  AND (B = Oregon)} 12";
        String test = command[0];
        FileScan fileScan = new FileScan(test);
        fileScan.execute();
    }

}
