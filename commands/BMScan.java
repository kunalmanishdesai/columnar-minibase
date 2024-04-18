package commands;

import columnar.BTIndexScan;
import columnar.Utils;
import heap.Tuple;

public class BMScan extends ScanCommand {

    public BTIndexScan btIndexScan;

    public BMScan(String input) {
        super(input);
        btIndexScan = new BTIndexScan(columnarFile, outputTupleAttributes);
    }

    @Override
    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = btIndexScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(count,tuple,attrTypes));
            count++;
        }

        btIndexScan.closeScan();
        System.out.println("Number of tuples printed: " + count);
    }


    public static void main(String[] command) {
//        String test = "testdb test1 [A,B,C,D,E] {(C = 2)} 30";
        String test = command[0];
        BMScan bmScan = new BMScan(test);
        bmScan.execute();
    }
}
