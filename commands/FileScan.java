package commands;

import columnar.TupleScan;
import columnar.Utils;
import diskmgr.PCounter;
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

    public static void main(String[] command) {

//        String test = "testdb test1 [A,B,C,D,E] {(C = 2)} 12";
        String test = command[0];

        PCounter.initialize();
        FileScan fileScan = new FileScan(test);
        fileScan.execute();
        PCounter.print();
    }
}
