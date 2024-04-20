package commands;

import bitmap.BitmapType;
import columnar.BTIndexScan;
import columnar.BTreeColumnarScan;
import columnar.Utils;
import heap.Tuple;
import iterator.CondExpr;
import iterator.TupleScanInterface;

import java.util.ArrayList;
import java.util.List;

public class BTScan extends ScanCommand {

    private final TupleScanInterface[] tupleScanInterfaces;

    public BTScan(String input) {
        super(input);
        tupleScanInterfaces = getScan(outputTupleAttributes.getCondExprs());
    }

    @Override
    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        for (TupleScanInterface tupleScanInterface : tupleScanInterfaces) {
            while ((tuple = tupleScanInterface.get_next()) != null) {
                System.out.println(Utils.getTupleString(count,tuple,attrTypes));
                count++;
            }

            tupleScanInterface.closeScan();
        }

        System.out.println("Number of tuples printed: " + count);
    }

    private TupleScanInterface[] getScan( CondExpr[] condExprs) {

        List<TupleScanInterface> tupleScanInterfaces = new ArrayList<>();

        if (condExprs[0].next != null) {

            CondExpr condExpr = condExprs[0];

            while (condExpr != null) {

                CondExpr[] tempCondExpr = new CondExpr[2];
                tempCondExpr[0] = new CondExpr(condExpr);
                tempCondExpr[0].next = null;

                if(columnarFile.getColumnFile(condExpr.operand1.symbol.offset-1).hasBtree()) {
                    tupleScanInterfaces.add(new BTreeColumnarScan(columnarFile,outputTupleAttributes,tempCondExpr));
                } else {
                    tupleScanInterfaces.add(new BTIndexScan(columnarFile,outputTupleAttributes,tempCondExpr,BitmapType.CBITMAP));
                }

                condExpr = condExpr.next;
            }
        }

        else if (columnarFile.getColumnFile(condExprs[0].operand1.symbol.offset-1).hasBtree()) {
            tupleScanInterfaces.add(new BTreeColumnarScan(columnarFile,outputTupleAttributes,condExprs));
        } else if ( condExprs[1] != null && columnarFile.getColumnFile(condExprs[1].operand1.symbol.offset-1).hasBtree()) {

            CondExpr temp = new CondExpr(condExprs[0]);
            temp.next = null;

            condExprs[0] = new CondExpr(condExprs[1]);
            condExprs[1] = temp;

            tupleScanInterfaces.add(new BTreeColumnarScan(columnarFile,outputTupleAttributes,condExprs));
        } else {
            tupleScanInterfaces.add(new BTIndexScan(columnarFile,outputTupleAttributes,BitmapType.CBITMAP));
        }

        return tupleScanInterfaces.toArray(new TupleScanInterface[] {});
    }

    public static void main(String[] command) {
//        String test = "testdb test1 [A,B,C,D,E] {(A = Iowa) AND (B = Zimbabwe)} 12";
        String test = command[0];
        BTScan btScan = new BTScan(test);
        btScan.execute();
    }
}
