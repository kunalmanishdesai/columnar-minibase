package commands;

import columnar.BTreeColumnarScan;
import columnar.Utils;
import heap.Tuple;

public class BTScan extends ScanCommand {

    private final BTreeColumnarScan bTreeColumnarScan;
    public BTScan(String input) {
        super(input);
        bTreeColumnarScan = new BTreeColumnarScan(columnarFile, outputTupleAttributes);
    }

    @Override
    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = bTreeColumnarScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(tuple,attrTypes));
            count++;
        }

        bTreeColumnarScan.closeScan();
        System.out.println("Number of tuples printed: " + count);
    }

    public static void main(String[] command) {
        String test = "testdb test1 [A,B,C,D] {(A = Washington )} 12";
//        String test = command[0];
        BTScan btScan = new BTScan(test);
        btScan.execute();
    }
}
