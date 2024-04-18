package commands;

import columnar.ColumnScan;
import columnar.Utils;
import heap.Tuple;

public class ColumnScanCommand extends ScanCommand {

    private final ColumnScan columnScan;
    public ColumnScanCommand(String inputCommand) {

        super(inputCommand);
        columnScan = new ColumnScan(columnarFile,outputTupleAttributes);
    }

    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = columnScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(count,tuple,attrTypes));
            count++;
        }

        columnScan.closeScan();
        System.out.println("Number of tuples printed: " + count);
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {

//        String test = "testdb test1 [A,B,C,D,E] {(C = 2)} 12";
        String test = command[0];
        ColumnScanCommand columnScan = new ColumnScanCommand(test);
        columnScan.execute();
    }

}
