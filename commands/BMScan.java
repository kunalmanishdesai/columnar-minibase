package commands;

import bitmap.BitmapType;
import columnar.BTIndexScan;
import columnar.Utils;
import diskmgr.PCounter;
import heap.Tuple;

public class BMScan extends ScanCommand {

    public BTIndexScan btIndexScan;

    public BMScan(String input,BitmapType bitmapType) {
        super(input);
        btIndexScan = new BTIndexScan(columnarFile, outputTupleAttributes, bitmapType);
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
//        String test = "testdb test1 [A,B,C,D] {(A > Arizona) AND ((B < Indiana)} 1000";
        String test = command[0];
        BitmapType bitmapType = BitmapType.valueOf(command[1]);
//        BitmapType bitmapType = BitmapType.CBITMAP;
        PCounter.initialize();
        BMScan bmScan = new BMScan(test,bitmapType);
        bmScan.execute();
        PCounter.print();
    }
}
